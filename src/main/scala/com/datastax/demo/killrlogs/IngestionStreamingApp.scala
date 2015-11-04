package com.datastax.demo.killrlogs


import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.streaming._
import TimeUtils._

/**
 * Created by Patrick on 19/10/15.
 */
// dse spark-submit killrlogs-streaming.jar --deploy-mode cluster --supervise
// dse spark-submit --class com.datastax.demo.killrlogs.IngestionStreamingApp killrlogs-streaming.jar


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
    val logs = stream
      .map(_._2.split(";"))
      .map( x => (x(0), x(1), getMinuteBucketTsFrom(x(2), 60), getMinuteBucketTsFrom(x(2), 1), x(3), x(4)))
      .cache()

    // TODO: filter malformed logs out
    
    logs.saveToCassandra("killrlog_ks", "logs", SomeColumns("id", "source_id", "bucket_ts", "ts", "type", "raw"))

    val counters = logs // 1 hour bucket, interval 60s
      .map(x => ((x._2, x._5, getMinuteBucketTsFrom(x._3, 3600)), (1, x._4)))
      // k(source, type, bucket_ts) v(1, date)
      .reduceByKey((x, y) => (x._1 + y._1, x._2))
      // k(source, type, bucket_ts) v(count, date)
      .map(x => (x._1._1, x._1._2, x._1._3, x._2._2, offsets, x._2._1))

    counters.print()
    counters.saveToCassandra("killrlog_ks", "counters", SomeColumns("source_id", "serie_id", "bucket_ts",  "ts", "id", "count"))

    ssc
  }

}