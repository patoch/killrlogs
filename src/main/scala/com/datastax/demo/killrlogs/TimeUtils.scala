package com.datastax.demo.killrlogs

import java.text.SimpleDateFormat
import java.util.Date


/**
 * Created by Patrick on 01/11/15.
 */
object TimeUtils {

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
