package com.emily


import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat

object MainTesting extends App {


  val time1 = System.currentTimeMillis()
  val sourcePath = s"/Users/bhavani.sankar/Desktop/Proj/Tasks/e_data/e_data"
  val startDate = "20190701"
  val endDate = "20190731"


  val originalFormat = DateTimeFormat.forPattern("yyyyMMdd")
  val newStartDate =  originalFormat.parseDateTime(startDate)
  val newEndDate = originalFormat.parseDateTime(endDate)

  val durationDays = Days
    .daysBetween(newStartDate,newEndDate).getDays()

  val tempS3Path = (0 to durationDays).map(date => {
    val newDate = newStartDate.plusDays(date)
    val newTargetDate = newDate.toString("yyyy-MM-dd")
    s"${newTargetDate}"
  })

  val finalS3Path = tempS3Path.mkString(",")
  val sessionSummaryLogsList = List("Main","Apps","Improvements","Selections","autoDiagnostic","manualDiagnostic")

  val sessionPaths = sessionSummaryLogsList.map(col => s"${sourcePath}/$col/dt={${finalS3Path}}/*")

  val clickEventsPath = s"${sourcePath}/ClickEvent/dt={${finalS3Path}}/*"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(s"Emily-ETL : ${args.mkString(", ")}")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val improvements = spark.sparkContext
    .wholeTextFiles(s"${sourcePath}/Improvements/*/*",20)

  val impRDD = improvements
    .flatMap(e => {
      val a = e._2.split("\\r?\\n").drop(1)
      val t = a.map(r => {
        val rec = r.split("\\|")
        (rec(0),rec(1),rec(2),rec(3),rec(4),rec(5),rec(6),rec(7),rec(8),rec(9),rec(10))
      })
      t
    })
    .toDS()

  val colNames = List("id","label","show","priority","test","isCompleted","startDate","endDate","symptoms_id","transactionId","s3FolderDate")



  val improvementsDS = impRDD.toDF(colNames:_*).repartition(20)


  improvementsDS.show(100,false)
  improvementsDS.printSchema()

  println(improvementsDS.select("transactionId").distinct().count())
  val time2 = System.currentTimeMillis()
  val turnAroundTime = (time2-time1) /1000
  println(time1,time2)
  println(s"total seconds : ${turnAroundTime}")

}
