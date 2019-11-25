package com.bhavani.scala

import org.joda.time.Months
import org.joda.time.format.DateTimeFormat

object FirstLastDateCalcuationBetweenGivenRangeMonths extends App {

  val inputPath =args(0)
  val startMonth =args(1)
  val endMonth = args(2)
  val outputPath = args(3)

  val originalFormat = DateTimeFormat.forPattern("yyyyMM")
  val parsedStartMonth = originalFormat.parseDateTime(startMonth)
  val parsedEndMonth =  originalFormat.parseDateTime(endMonth)
  val durationMonths = Months.monthsBetween(parsedStartMonth,parsedEndMonth).getMonths()

  val tempS3Path =(0 to durationMonths).map(i => {
  
    val currMonth = parsedStartMonth.plusMonths(i)
    val currMonthStartDate = currMonth.dayOfMonth().withMinimumValue().toString("yyyy-MM-dd") + " 00:00:00"
    val currMonthLastDate = currMonth.dayOfMonth().withMaximumValue().toString("yyyy-MM-dd") + " 23:59:59"
    println(currMonth,currMonthStartDate,currMonthLastDate)

    //s"$val currMonth = parsedStartMonth.plusMonths(i).toString("yyyy-MM")-*"

  })
  val finalS3Path = tempS3Path.mkString(",")

  //val finalUserEventPath =s"${userEventPath}/{$finalS3Path}"
  val finalDeviceReportPath = s"${deviceReportPath}/{$finalS3Path}"
}
