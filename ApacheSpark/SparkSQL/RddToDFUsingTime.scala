package com.bhavani.asurion

import org.apache.spark.sql.SparkSession
import org.joda.time.Months
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions.split

object MonthWiseAppUseLiteracyCount extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(s"MonthWiseAppUsersCount: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._



  val path = s"/18_month_wise_app_users_count/month_wise_s3_data"

  val startMonth= "201705"
  val endMonth="201905"

  val originalFormat = DateTimeFormat.forPattern("yyyyMM")
  val newStartDate = originalFormat.parseDateTime(startMonth)
  val newEndDate= originalFormat.parseDateTime(endMonth)

  //println(newStartDate + "\n" +newEndDate)

  val durationMonths = Months.monthsBetween(newStartDate,newEndDate).getMonths()
  //println(durationMonths)

  val monthsCounts = (0 to durationMonths).map(month => {

    val currentMonth = newStartDate.plusMonths(month)
    val currentMonthStr = currentMonth.toString("yyyy-MM")
    val currentMonthPath = s"${path}/${currentMonthStr}/"
    //println(currentMonthPath)
    val df = spark.read.parquet(currentMonthPath)
    //println(s"${currentMonthStr},${df.count()}")
    s"${currentMonthStr},${df.count()}"

    })

  val monthResRDD = spark.sparkContext.parallelize(monthsCounts)
  //println(monthResRDD)
  //println(monthResRDD.count())
  //monthResRDD.foreach(println(_))


  val monthsResDF = monthResRDD.toDF()
      .withColumn("month",split($"value",",").getItem(0))
      .withColumn("count",split($"value",",").getItem(1))
      .drop($"value")
  monthsResDF.show(false)
  monthsResDF.printSchema()

}
