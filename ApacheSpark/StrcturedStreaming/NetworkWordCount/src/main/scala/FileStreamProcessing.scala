package com.bhavani

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object FileStreamProcessing extends App{

  val spark = SparkSession
    .builder
    .appName("File Stream Processing")
    .master("local[4]")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference","true")
    .config("spark.sql.shuffle.partitions",4)
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

val rawDF = spark.readStream
  .format("json")
  .option("path","sample_data")
  .load()

  //println("raw data schema")
  //rawDF.printSchema()

  val explodeDF = rawDF
    .selectExpr(
      "id",
      "name",
      "price",
      "requires_shipping",
      "sku",
      "status",
      "taxable",
      "vendor.id",
      "vendor.name",
      "explode(details) as details"
  )

  //println("explode data schema")
  //explodeDF.printSchema()

  val flattendDF = explodeDF
    .withColumn("album_id",expr("details.album_id"))
    .withColumn("bytes",expr("details.bytes"))
    .withColumn("composer",expr("details.composer"))
    .withColumn("genre_id",expr("details.genre_id"))
    .withColumn("media_type_id",expr("details.media_type_id"))
    .withColumn("milliseconds",expr("details.milliseconds"))
    .withColumn("name",expr("details.name"))
    .withColumn("track_id",expr("details.track_id"))
    .withColumn("unit_price",expr("details.unit_price"))
    .drop("details")

  //println("flatten data schema")
  //flattendDF.printSchema()

  val invoiceWriteQuery = flattendDF
    .writeStream
    .format("json")
    .option("path","output")
    .option("checkpointLocation","chk-point-dir")
    .outputMode("append")
    .queryName("Flattend Invoice Query writer")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start()

  println("Flatten Invoice Writer started....")
  invoiceWriteQuery.awaitTermination()

}
