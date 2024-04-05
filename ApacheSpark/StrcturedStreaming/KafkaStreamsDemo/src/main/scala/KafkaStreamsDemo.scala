package com.bhavani.spark.streaming.kafka

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
object KafkaStreamsDemo extends App with Serializable{

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession
    .builder()
    .appName("Kafka Demo")
    .master("local[*]")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()

  import spark.implicits._

  val schema = Schema.schema

  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","invoices")
    .option("startingOffsets","earliest")
    .load()

  val valueDF = kafkaDF
    .select(
      from_json($"value".cast("string"),schema).as("value")

  )

  val explodeDF = valueDF
    .selectExpr(
    "value.InvoiceNumber","value.CreatedTime","value.StoreID","value.PosID",
    "value.CustomerType","value.PaymentMethod","value.DeliveryType","value.DeliveryAddress.city",
    "value.DeliveryAddress.State","value.DeliveryAddress.PinCode","value.DeliveryAddress.ContactNumber",
    "explode(value.InvoiceLineItems) as LineItem"
  )

  val flattenDF = explodeDF
    .withColumn("ItemCode",$"LineItem.ItemCode")
    .withColumn("ItemDescription",$"LineItem.ItemDescription")
    .withColumn("ItemPrice",$"LineItem.ItemPrice")
    .withColumn("ItemQty",$"LineItem.ItemQty")
    .withColumn("TotalValue",$"LineItem.TotalValue")
    .drop("LineItem")

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


  kafkaDF.printSchema()
}
