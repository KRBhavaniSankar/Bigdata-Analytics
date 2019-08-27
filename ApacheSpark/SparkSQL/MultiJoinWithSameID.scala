package com.bhavani

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataAnalysis extends App {


  def createDataFrame(path:String,append:String):DataFrame ={

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "|")
        .csv(path)

      val selectColumns = df.columns.toList

      val modifiedDF = if (selectColumns.contains("dt")) df.drop("dt") else df

      val newColumns = modifiedDF.columns.toList.map(col => s"${append}_${col}")

      modifiedDF.toDF(newColumns: _*)

  }

  val spark = SparkSession.builder()
      .appName(s"DataAnalysis : ${args.mkString(", ")}")
      .master("local[*]")
      .getOrCreate()

  val sourcePath = s"data_20190826"

  spark.sparkContext.setLogLevel("ERROR")

  val sessionSummaryLogsList = List("Main","Apps","Improvements","Selections","autoDiagnostic","manualDiagnostic")

  val paths = sessionSummaryLogsList.map(col => s"${sourcePath}/SessionSummary/$col")
  val clickEventsPath = s"${sourcePath}/ClickEvent"
  val mainDF =  createDataFrame(paths(0),"main")
  val appsDF = createDataFrame(paths(1),"apps")
  val improvementsDF = createDataFrame(paths(2),"imp")
  val selectionsDF = createDataFrame(paths(3),"sel")
  val autoDiagnosticDF = createDataFrame(paths(4),"ad")
  val manualDiagnosticDF = createDataFrame(paths(5),"md")
  val clickEventsDF = createDataFrame(clickEventsPath,"ce")

  val joinedDS = mainDF
    .join(appsDF, mainDF("main_transactionId") === appsDF("apps_transactionId"),"inner")
    .join(improvementsDF, mainDF("main_transactionId") === improvementsDF("imp_transactionId"),"inner")
    .join(selectionsDF, mainDF("main_transactionId") === selectionsDF("sel_transactionId"),"inner")
    .join(autoDiagnosticDF, mainDF("main_transactionId") === autoDiagnosticDF("ad_transactionId"),"inner")
    .join(manualDiagnosticDF, mainDF("main_transactionId") === manualDiagnosticDF("md_transactionId"),"inner")
    .join(clickEventsDF, mainDF("main_transactionId") === clickEventsDF("ce_transactionId"),"inner")

  //joinedDS.show(100,false)
  //joinedDS.printSchema()

  //println(joinedDS.select("main_transactionId").distinct().count())     //81
  //println(joinedDS.count())     //994976230

  val cols = joinedDS.columns.toList.filter(col => col.contains("transactionId"))

  val colsToRemove = cols.tail
  val transactionWiseDF = joinedDS.drop(colsToRemove: _*)

  val outputPath = s"/Users/bhavani.sankar/Desktop/Proj/Asurion/Tasks/emily_data/tr_id_wise_20190827"

  transactionWiseDF.repartition(transactionWiseDF("main_transactionId"))
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("main_transactionId")
    .parquet(outputPath)

  println("All done !!")

}
