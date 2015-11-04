package com.datastax.demo.killrlogs

import java.text.SimpleDateFormat
import java.util.Date


/**
 * Created by Patrick on 01/11/15.
 */
object TimeUtils {

  val formatTmp = "yyyy-MM-dd HH:mm:ssZ"

  def getMinuteBucketTsFrom(datetime: String, bucketSizeInMinutes: Int): Date = {
    val format = new SimpleDateFormat(formatTmp)
    getMinuteBucketTsFrom(format.parse(datetime), bucketSizeInMinutes)
  }

  def getMinuteBucketTsFrom(datetime: Date, bucketSizeInMinutes: Int): Date = {
    val ts = datetime.getTime()
    new Date(ts - (ts % (bucketSizeInMinutes * 60000)))
  }

  def getTsFrom(datetime: String): Date = {
    new SimpleDateFormat(formatTmp).parse(datetime)
  }

  def formatDate(datetime:Date):String = {
    new SimpleDateFormat(formatTmp).format(datetime)
  }

  def formatDate(datetime:Date, formatTmp: String):String = {
    new SimpleDateFormat(formatTmp).format(datetime)
  }

}
