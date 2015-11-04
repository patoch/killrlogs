package com.datastax.demo.killrlogs

import java.util.{Calendar, Date}

import com.datastax.spark.connector.cql._
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import TimeUtils._

import scala.collection.mutable.ArrayBuffer

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

  // ----------- rollup hours since last rollup
  val lastRollupTs = getMinuteBucketTsFrom("2015-11-03 11:00:00+0000", 60)
  println(s"Rolling up from $lastRollupTs")


  /* ALTERNATIVE WITH FULL SCAN

  val counters = sc.cassandraTable[(UUID, String, Date, Int)]("killrlog_ks", "counters")
    .select("source_id", "serie_id", "ts", "count")
    .where("ts >= ?", lastRollupTs)

  counters.map(x => ((x._1, x._2, getMinuteBucketTsFrom(x._3, 60)), (x._3, x._4)))
    .reduceByKey((x, y) => (x._1, x._2 + y._2))
    .map(x => (x._1._1, x._1._2, getMinuteBucketTsFrom(x._1._3, 60 * 24), x._1._3, x._2._2))
    .saveToCassandra("killrlog_ks", "counter_rollups_1h", SomeColumns("source_id", "serie_id", "bucket_ts", "ts", "count"))

  */

  val counters = getRawCounterRDD()

  // 1.1:source, 1.2:serie, 1.3:hourly_bucket, 2 count
  val r = counters
    .map((x)=>((x._1._1, x._1._2, x._1._3), x._2))
    .reduceByKey(_+_)
    .map(x => (x._1._1, x._1._2, getMinuteBucketTsFrom(x._1._3, 60 * 24), x._1._3, x._2))
    .saveToCassandra("killrlog_ks", "counter_rollups_1h", SomeColumns("source_id", "serie_id", "bucket_ts", "ts", "count"))


  // return an rdd of raw hourly counters
  def getRawCounterRDD() = {

    var sourceIds = ArrayBuffer[String]()//Array("9d6e4330-8203-11e5-9c43-cf678cff6bd8", "e98a40c0-824e-11e5-9813-cf678cff6bd8")
    var serieIds = ArrayBuffer[String]()//Array("view_category", "search", "buy_product", "like_product", "view_product")
    var bucketTss = ArrayBuffer[String]()

    val sources = sc.cassandraTable[String]("killrlogs_ks", "reference").select("name").where("id='logtypes")
    val logtypes = sc.cassandraTable[String]("killrlogs_ks", "reference").select("name").where("id='logtypes'")


    val currentTime = new Date(System.currentTimeMillis)
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(lastRollupTs)

    while (cal.getTime.compareTo(currentTime) < 0) {
      cal.add(Calendar.HOUR, 1)
      bucketTss += formatDate(cal.getTime)
    }

    // create an array of partition keys as tuples, which are all combinations of sources, series and buckets
    var partitionKeys = Seq[(String, String, String)]()
    for (sourceId <- sourceIds) {
      for (serieId <- serieIds) {
        for (bucketTs <- bucketTss) {
          partitionKeys = partitionKeys :+ (sourceId, serieId, bucketTs)
        }
      }
    }

    //val partitionKeys = Seq((UUID.fromString("9d6e4330-8203-11e5-9c43-cf678cff6bd8"), "search", getTsFrom("2015-11-03 12:00:00:000")))

    val partitionCount = partitionKeys.size
    println(s"$partitionCount partitions selected for level 1 rollup")

    val rdd = sc.parallelize(partitionKeys)
      .repartitionByCassandraReplica("killrlog_ks","counters")
      .joinWithCassandraTable[Int]("killrlog_ks", "counters", SomeColumns("count"))
      .on(SomeColumns("source_id", "serie_id", "bucket_ts"))

    println(rdd.count() + " partitions found.")

    rdd
  }

}
