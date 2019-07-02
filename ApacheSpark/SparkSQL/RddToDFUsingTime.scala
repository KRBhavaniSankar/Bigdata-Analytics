package com.bhavani.asurion

import org.apache.spark.sql.SparkSession
import org.joda.time.Months
import org.joda.time.format.DateTimeFormat
object MonthWiseAppUseLiteracyCount extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(s"MonthWiseAppUsersCount: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._



  val appUsePath = s"18_month_wise_app_users_count/month_wise_s3_data"
  val kjUsersPath = s"18_month_wise_app_users_count/KJ_month_wise_unique_users"

  val startMonth= "201705"
  val endMonth="201902"

  val originalFormat = DateTimeFormat.forPattern("yyyyMM")
  val newStartDate = originalFormat.parseDateTime(startMonth)
  val newEndDate= originalFormat.parseDateTime(endMonth)


  val durationMonths = Months.monthsBetween(newStartDate,newEndDate).getMonths()

  val monthsCounts = (0 to durationMonths).map(month => {

    val currentMonth = newStartDate.plusMonths(month)
    val currentMonthStr = currentMonth.toString("yyyy-MM")
    val currentMonthAppUsersPath = s"${appUsePath}/${currentMonthStr}/"
    val currentMonthKJUsersPath = s"${kjUsersPath}/ym=${currentMonthStr}"
    val appUsersDF = spark.read.parquet(currentMonthAppUsersPath).distinct()
    val kjUsersDF = spark.read.option("header","true").csv(currentMonthKJUsersPath).distinct()

    val commonUsers = appUsersDF.join(kjUsersDF,appUsersDF("imei")===kjUsersDF("アカウント識別子"),"inner")
      //.drop("imei")
      .distinct()
    //println(currentMonthStr)
    //commonUsers.show(10,false)


    println(s"${currentMonthStr}\t${appUsersDF.count()}\t${kjUsersDF.count()}\t${commonUsers.count()}")

    //println(currentMonthAppUsersPath+"\n"+currentMonthKJUsersPath)


    //println(s"${currentMonthStr},${df.count()}")
    //s"${currentMonthStr},${appUsersDF.count()}"
    //s"${currentMonthStr}\t${appUsersDF.count()}\t${kjUsersDF.count()}\t${commonUsers.count()}"

    })

  val monthResRDD = spark.sparkContext.parallelize(monthsCounts)


/*
  val monthsResDF = monthResRDD.toDF()
      .withColumn("year-month",split($"value","\t").getItem(0))
      .withColumn("app_use_unique_users_count",split($"value","\t").getItem(1))
      .withColumn("kj_users_unique_count",split($"value","\t").getItem(2))
      .withColumn("common_users_count",split($"value","\t").getItem(3))
      .drop($"value")
  monthsResDF.show(false)
  monthsResDF.printSchema()*/

}
