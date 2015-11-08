package com.datastax.demo.killrlogs


import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.streaming._
import com.datastax.demo.killrlogs.TimeUtils._

/**
 * Created by Patrick on 19/10/15.
 */
// dse spark-submit --deploy-mode cluster --supervise --class com.datastax.demo.killrlogs.IngestionStreamingApp killrlogs-streaming.jar


object IngestionStreamingApp extends App {

  val cassandraContactPoints = Some(sys.env("CASSANDRA_CONTACT_POINTS")).getOrElse("127.0.0.1")
  val sparkMaster = Some(sys.env("SPARK_MASTER")).getOrElse("spark://127.0.0.1:7077")
  val sparkExecutorMemory = Some(sys.env("kl_executor_memory")).getOrElse("1g")
  val sparkCores = Some(sys.env("kl_cores")).getOrElse("3")

  val group = "log.streaming.g1"
  val checkpointDir = "/killrlogs/job1/checkpoint"
  val topicsSet = "logs".split(",").toSet
  val brokers = Some(sys.env("kl_brokers")).getOrElse("127.0.0.1:9092")
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

  val batchIntervalInSeconds = 60

  // create spark configuration
  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .setMaster(sparkMaster)
    .set("spark.executor.memory", sparkExecutorMemory)
    .set("spark.cores.max", sparkCores)
    .set("spark.cassandra.connection.host", cassandraContactPoints)

  // create streaming context and streams and start
  val ssc =   StreamingContext.getOrCreate(checkpointDir, () => {createStreamingContext(conf)})
  ssc.start()
  ssc.awaitTermination()


  def createStreamingContext(conf: SparkConf) = {
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(batchIntervalInSeconds))
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Set metadata checkpointing on
    ssc.checkpoint(checkpointDir)

    // find current batch interval Kafka offsets
    var offsetRanges = List[OffsetRange]()
    var offsets = ""

    stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.toList
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        offsets  = s"${o.partition}-${o.fromOffset}-${o.untilOffset}"
        println(s"Offsets:  ${o.fromOffset} - ${o.untilOffset} (topic ${o.topic}, partition ${o.partition})")
      }
    }

    // Aggregate logs in 60 seconds buckets
    val mapStreamToLogAsTuple: ((String, String)) => (String, String, java.util.Date, java.util.Date, String, String) = {
      case (k, v) => {
        val data = v.split(";")
        // TODO: validate log and append flag for later filtering of malformed logs
        (data(0), data(1), getMinuteBucketTsFrom(data(2), 60), getMinuteBucketTsFrom(data(2), 1), data(3), data(4))
      }
    }

    val logs = stream
      .map(mapStreamToLogAsTuple)
      .cache()

    // TODO: filter invalid logs out and store to invalid_logs table

    logs.saveToCassandra("killrlog_ks", "logs", SomeColumns("id", "source_id", "bucket_ts", "ts", "type", "raw"))

    val counters = logs // 1 hour bucket, interval 60s
      .map {case (id, sourceId, bucketTs, ts, logtype, raw) =>
        ((sourceId, logtype, getMinuteBucketTsFrom(bucketTs, 3600)), (1, ts))}
      .reduceByKey { case ((reducedCount, reducedTs), (count, ts)) =>
        (reducedCount + count, reducedTs) }
      .map {case ((sourceId, logtype, bucketTs),(count, ts)) =>
        (sourceId, logtype, bucketTs, ts, offsets, count)}

    counters.print()
    counters.saveToCassandra("killrlog_ks", "counters", SomeColumns("source_id", "serie_id", "bucket_ts",  "ts", "id", "count"))

    ssc
  }

}