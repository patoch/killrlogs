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

  // Rollup workload partitioning
  val partitionCount = 1 // number of partitions, basically number of analytical data centers
  val ownedPartition = 0

  // Create spark configuration
  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .setMaster(sparkMaster)
    .set("spark.executor.memory", sparkExecutorMemory)
    .set("spark.cores.max", sparkCores)
    .set("spark.cassandra.connection.host", cassandraContactPoints)

  val sc = new SparkContext(conf)

  // Pickup from last rollup
  val lastRollupTs = getLastRollupTs()
  println(s"[ ROLLING UP COUNTERS SINCE $lastRollupTs ]")

  // Fetch some reference data
  val sourceIds = getSourceIds()
  val serieIds = getLogTypes()

  // Rollup hours since last rollup
  val counters = getRawCounterRDD()

  counters
    // 1.1:source, 1.2:serie, 1.3:hourly_bucket, 2 count
    .map((x)=>((x._1._1, x._1._2, x._1._3), x._2))
    .reduceByKey(_+_)
    .map(x => (x._1._1, x._1._2, getMinuteBucketTsFrom(x._1._3, 60 * 24), x._1._3, x._2))
    .saveToCassandra("killrlog_ks", "counter_rollups_1h", SomeColumns("source_id", "serie_id", "bucket_ts", "ts", "count"))


  // Rollup days on hourly rollups
  val counterRollup1h = getHourlyCounterRDD()

  counterRollup1h.map((x)=>((x._1._1, x._1._2, x._1._3), x._2))
    .reduceByKey(_+_)
    .map(x => (x._1._1, x._1._2, getMonthBucketTsFrom(getTsFrom(x._1._3), 60 * 24), x._1._3, x._2))
    .saveToCassandra("killrlog_ks", "counter_rollups_1d", SomeColumns("source_id", "serie_id", "bucket_ts", "ts", "count"))

  // Store rollup ts
  setLastRollupTs()

  // ----------------------- THE END


  // Returns an rdd of raw hourly counters to be summed up
  // Partition keys are generated as sourceIds x serieIds x buckets
  // The RDD is repartitioned by cassandra replica and a local join is made to fetch the data
  def getRawCounterRDD() = {

    /* ALTERNATIVE WITH FULL SCAN

    val counters = sc.cassandraTable[(UUID, String, Date, Int)]("killrlog_ks", "counters")
      .select("source_id", "serie_id", "ts", "count")
      .where("ts >= ?", lastRollupTs)

    counters.map(x => ((x._1, x._2, getMinuteBucketTsFrom(x._3, 60)), (x._3, x._4)))
      .reduceByKey((x, y) => (x._1, x._2 + y._2))
      .map(x => (x._1._1, x._1._2, getMinuteBucketTsFrom(x._1._3, 60 * 24), x._1._3, x._2._2))
      .saveToCassandra("killrlog_ks", "counter_rollups_1h", SomeColumns("source_id", "serie_id", "bucket_ts", "ts", "count"))

    */

    // List buckets to be fetched
    var bucketTss = ArrayBuffer[String]()

    val currentTime = new Date(System.currentTimeMillis)
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(lastRollupTs)

    while (cal.getTime.compareTo(currentTime) < 0) {
      bucketTss += formatDate(cal.getTime)
      cal.add(Calendar.HOUR, 1)
    }

    // create an array of partition keys as tuples, which are all combinations of sources, series and buckets
    var partitionKeys = Seq[(String, String, String)]()
    for (sourceId <- sourceIds) {
      for (serieId <- serieIds) {
        for (bucketTs <- bucketTss) {
          if (isPartitionOwner((sourceId, serieId, bucketTs))) {
            partitionKeys = partitionKeys :+(sourceId, serieId, bucketTs)
          }
        }
      }
    }

    val partitionCount = partitionKeys.size
    println(s"Level 1 rollups: $partitionCount partitions")

    sc.parallelize(partitionKeys)
      .repartitionByCassandraReplica("killrlog_ks","counters")
      .joinWithCassandraTable[Int]("killrlog_ks", "counters", SomeColumns("count"))
      .on(SomeColumns("source_id", "serie_id", "bucket_ts"))

  }


  def getHourlyCounterRDD() = {

    var bucketTss = ArrayBuffer[String]()

    val currentTime = new Date(System.currentTimeMillis)
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(getDayBefore(getMinuteBucketTsFrom(lastRollupTs, 24 * 60)))

    while (cal.getTime.compareTo(currentTime) < 0) {
      bucketTss += formatDate(cal.getTime)
      cal.add(Calendar.HOUR, 24)
    }

    // create an array of partition keys as tuples, which are all combinations of sources, series and buckets
    var partitionKeys = Seq[(String, String, String)]()
    for (sourceId <- sourceIds) {
      for (serieId <- serieIds) {
        for (bucketTs <- bucketTss) {
          if (isPartitionOwner((sourceId, serieId, bucketTs))) {
            partitionKeys = partitionKeys :+(sourceId, serieId, bucketTs)
          }
        }
      }
    }

    val partitionCount = partitionKeys.size
    println(s"Level 2 rollups: $partitionCount partitions")

    sc.parallelize(partitionKeys)
      .repartitionByCassandraReplica("killrlog_ks","counter_rollups_1h")
      .joinWithCassandraTable[Int]("killrlog_ks", "counter_rollups_1h", SomeColumns("count"))
      .on(SomeColumns("source_id", "serie_id", "bucket_ts"))

  }

  // Returns last rollup timestamp
  // If none is stored, goes 24 hours
  // If one is stored, would return it one hour before, to rollup counters that may have arrived since then.
  // Returned time is aligned on an hour, so that rollups can be made several time in a given hour
  def getLastRollupTs() = {
    CassandraConnector(conf).withSessionDo { session =>
      val lastRollupIter = session.execute("SELECT blobAsTimestamp(value) AS ts FROM killrlog_ks.references WHERE key='rollups' AND name='lastRollupTs';").iterator()
      if (!lastRollupIter.hasNext) {
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.HOUR, -24)
        getMinuteBucketTsFrom(cal.getTime, 60)
      } else {
        val cal: Calendar = Calendar.getInstance()
        cal.setTime(lastRollupIter.next().getDate("ts"))
        cal.add(Calendar.HOUR, -1)
        getMinuteBucketTsFrom(cal.getTime, 60)
      }
    }
  }

  // Store the last rollup ts to C*
  def setLastRollupTs() = {
    CassandraConnector(conf).withSessionDo { session =>
      val ts = formatDate(new Date())
      session.execute(s"INSERT INTO killrlog_ks.references(key, name, value) VALUES ('rollups', 'lastRollupTs', timestampAsBlob('$ts'));")
    }
  }


  def getSourceIds() = {
    var sourceIds = ArrayBuffer[String]()
    CassandraConnector(conf).withSessionDo { session =>
      val sourcesIter = session.execute("SELECT name FROM killrlog_ks.references WHERE key='sources';").iterator()
      while (sourcesIter.hasNext()) {
        sourceIds += sourcesIter.next().getString("name")
      }
    }
    sourceIds
  }

  def getLogTypes() = {
    var logtypes = ArrayBuffer[String]()
    CassandraConnector(conf).withSessionDo { session =>
      val logtypesIter = session.execute("SELECT name FROM killrlog_ks.references WHERE key='logtypes';").iterator()
      while (logtypesIter.hasNext()) {
        logtypes += logtypesIter.next().getString("name")
      }
    }
    logtypes
  }

  // Returns true if this node is assigned this partition
  def isPartitionOwner(key:(String,String,String)) = {
    ownedPartition == MurmurHash3.stringHash(s"$key._1:$key._2:$key._3") % partitionCount
  }


}
