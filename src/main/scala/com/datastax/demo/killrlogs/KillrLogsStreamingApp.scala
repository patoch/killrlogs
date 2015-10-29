package com.datastax.demo.killrlogs

import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.streaming._


/**
 * Created by Patrick on 19/10/15.
 */
// dse spark-submit killrlogs-streaming.jar --deploy-mode cluster --supervise
// /opt/kafka/bin/kafka-console-consumer.sh --zookeeper 10.240.0.2:2181 --topic logs --from-beginning

object KillrLogsStreamingApp extends App {


  val sparkMaster = Some(sys.env("SPARK_MASTER")).getOrElse("spark://127.0.0.1:7077")
  val sparkExecutorMemory = Some(sys.env("kl_executor_memory")).getOrElse("1g")
  val sparkCores = Some(sys.env("kl_cores")).getOrElse("3")
  val cassandraContactPoints = Some(sys.env("CASSANDRA_CONTACT_POINTS")).getOrElse("127.0.0.1")

  val brokers = Some(sys.env("kl_brokers")).getOrElse("127.0.0.1:9092")
  val topics = "logs"
  val group = "log.streaming.g1"
  val checkpointDir = "cfs://killrlogs/streaming/checkpoint"

  val dc = 1
  val batchIntervalInSeconds = 10

  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .setMaster(sparkMaster)
    .set("spark.executor.memory", sparkExecutorMemory)
    .set("spark.cores.max", sparkCores)
    .set("spark.cassandra.connection.host", cassandraContactPoints)

  val sc = new SparkContext(conf)
  val ssc =   StreamingContext.getOrCreate(checkpointDir, () => {createStreamingContext()})

  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

  val stream = createStream()

  val logs = stream // 5 second buckets
    .map(_._2.split(";"))
    .map( x => (x(0), x(1), getBucketTsFrom(x(2), 5), getTsFrom(x(2)), x(3), x(4)) )
    .cache()

  logs.saveToCassandra("killrlog_ks", "logs", SomeColumns("id", "source_id", "bucket_ts", "ts", "type", "raw"))

  val logs_kv = logs // 1 day bucket, interval 5s
      .map(x => ((x._2, x._5, getBucketTsFrom(x._3, 1440), dc), (1, x._4)))
      // k(source, type, bucket_ts, dc) v(1, date)
      .reduceByKey((x, y) => (x._1 + y._1, x._2))
      // k(source, type, bucket_ts, dc) v(count, date)
      .map(x => (x._1._1, x._1._2, x._1._3, x._2._2, x._1._4, UUIDs.random(), x._2._1))

  logs_kv.print()
  logs_kv.saveToCassandra("killrlog_ks", "counters", SomeColumns("source_id", "serie_id", "bucket_ts",  "ts", "dc", "id", "count"))

  ssc.start()
  ssc.awaitTermination()

  def createStreamingContext() = {
    val ssc =  new StreamingContext(sc, Seconds(batchIntervalInSeconds))
    ssc.checkpoint(checkpointDir)
    ssc
  }

  def createStream() = {
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    // for statefull rdd, checkpoint every 5 to 10 time the batch interval
    //stream.checkpoint(Seconds(10 * batchIntervalInSeconds))
    stream
  }

  def getBucketTsFrom(datetime: String, bucketSizeInMinutes: Int): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    getBucketTsFrom(format.parse(datetime), bucketSizeInMinutes)
  }

  def getBucketTsFrom(datetime: Date, bucketSizeInMinutes: Int): Date = {
    val ts = datetime.getTime()
    new Date(ts - (ts % (bucketSizeInMinutes * 60 * 1000)))
  }

  def getTsFrom(datetime: String): Date = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(datetime)
  }
  
}