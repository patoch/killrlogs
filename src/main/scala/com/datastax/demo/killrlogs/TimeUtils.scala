package com.datastax.demo.killrlogs

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


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

  def getMonthBucketTsFrom(datetime: Date, bucketSizeInMinutes: Int): Date = {
    val cal = Calendar.getInstance()
    cal.setTime(datetime)
    cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
    cal.getTime
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

  def getDayBefore(day: Date): Date = {
    val cal = Calendar.getInstance()
    cal.setTime(day)
    cal.add(Calendar.HOUR, -24)
    cal.getTime
  }

  def getDayAfter(day: Date): Date = {
    val cal = Calendar.getInstance()
    cal.setTime(day)
    cal.add(Calendar.HOUR, 24)
    cal.getTime
  }

}
