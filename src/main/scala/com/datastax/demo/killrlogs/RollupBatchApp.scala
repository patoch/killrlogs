package com.datastax.demo.killrlogs

import java.util.{Date, UUID}

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import TimeUtils._

/**
 * Created by Patrick on 01/11/15.
 */
//dse spark-submit --class com.datastax.demo.killrlogs.RollupBatchApp killrlogs-streaming.jar
object RollupBatchApp extends App {

  val cassandraContactPoints = Some(sys.env("CASSANDRA_CONTACT_POINTS")).getOrElse("127.0.0.1")
  val sparkMaster = Some(sys.env("SPARK_MASTER")).getOrElse("spark://127.0.0.1:7077")

  val sparkExecutorMemory = Some(sys.env("kl_rollups_executor_memory")).getOrElse("1g")
  val sparkCores = Some(sys.env("kl_rollups_cores")).getOrElse("3")


  // create spark configuration
  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .setMaster(sparkMaster)
    .set("spark.executor.memory", sparkExecutorMemory)
    .set("spark.cores.max", sparkCores)
    .set("spark.cassandra.connection.host", cassandraContactPoints)

  val sc = new SparkContext(conf)

  val lastRollupTs = "2015-11-01 00:00:00"

  val counters = sc.cassandraTable[(UUID, String, Date, Int)]("killrlog_ks", "counters")
    .select("source_id", "serie_id", "ts", "count")
    .where("ts >= ?", lastRollupTs)

  counters.map(x => ((x._1, x._2, getBucketTsFrom(x._3, 60 * 24)), (x._3, x._4)))
    .reduceByKey((x, y) => (x._1, x._2 + y._2))
    .map(x => (x._1._1, x._1._2, x._1._3, x._2._1, x._2._2))
    .saveToCassandra("killrlog_ks", "counter_rollups1h", SomeColumns("source_id", "serie_id", "bucket_ts", "ts", "count"))


}
