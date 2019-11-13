package com.bhavani.scala

import org.joda.time.Days
import org.joda.time.format.DateTimeFormat

object TOKAppUseLogs20191113 extends App {

  val inputPath = args(1)
  val startDate = args(2)
  val endDate = args(3)
  val originalFormat = DateTimeFormat.forPattern("yyyyMMdd")
  val firstDay = originalFormat.parseDateTime(startDate)
  val endDay =originalFormat.parseDateTime(endDate)
  val durationDays = Days.daysBetween(firstDay,endDay).getDays()

  val FilterStartDate = firstDay.toString("yyyy-MM-dd") + " 00:00:00"
  val FilterEndDate = endDay.toString("yyyy-MM-dd") + " 23:59:59"

  val modifiedPath = (0 to durationDays).map(date => {
    val newDate = firstDay.plusDays(date)
    val newTargetDate = newDate.toString("yyyy-MM-dd")
    s"${newTargetDate}/*"
  })

  val finalPath = modifiedPath.mkString(",")
  val finalInputPath = s"${inputPath}/{${finalPath}}"

  println(finalInputPath)

}
