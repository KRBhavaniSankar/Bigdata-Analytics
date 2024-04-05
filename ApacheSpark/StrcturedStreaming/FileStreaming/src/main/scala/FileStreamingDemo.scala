package com.bhavani.spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object FileStreamingDemo extends App with Serializable{

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession
    .builder()
    .appName("FileStreaming Demo")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference","true")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val rawDF = spark.readStream
    .format("json")
    .option("path","input")
    .option("maxFilesPerTrigger",1)
    .load()

  //rawDF.printSchema()

  //rawDF.show(10,false)

  val explodeDF = rawDF.selectExpr(
    "InvoiceNumber","CreatedTime","StoreID","PosID",
    "CustomerType","PaymentMethod","DeliveryType","DeliveryAddress.city",
    "DeliveryAddress.State","DeliveryAddress.PinCode","DeliveryAddress.ContactNumber",
    "explode(InvoiceLineItems) as LineItem"
  )

  val flattenDF = explodeDF.
    withColumn("ItemCode",$"LineItem.ItemCode").
    withColumn("ItemDescription",$"LineItem.ItemDescription").
    withColumn("ItemPrice",$"LineItem.ItemPrice").
    withColumn("ItemQty",$"LineItem.ItemQty").
    withColumn("TotalValue",$"LineItem.TotalValue").
    drop("LineItem")

  //flattenDF.printSchema()


  val invoiceWriterQuery = flattenDF.writeStream
    .format("json")
    .option("path","output")
    .option("checkpointLocation","chk-point-dir")
    .outputMode("append")
    .queryName("Flattened Invoice Query Writer")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start()

  logger.info("Flattened Invoice Writer started...")

  invoiceWriterQuery.awaitTermination()







}
