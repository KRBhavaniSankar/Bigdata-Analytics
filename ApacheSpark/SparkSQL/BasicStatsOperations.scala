import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object UserEventLogsEDA extends App {


val sampleUserPath = args(0)

val spark = SparkSession.builder()
  .master("local[*]")
  .appName(s"AppUseLiteracyEDA: ${args.mkString(",")}")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._

val sampleUserDF = spark.read.option("mergeSchema","true").parquet(sampleUserPath)
//sampleUserDF.show(50,false)
//sampleUserDF.printSchema()

val validRecord = sampleUserDF.select("id","model","event","event_date","imei")
  .filter($"event_date".between("2017-03-29 00:00:00","2018-10-31 23:59:59"))
  .sort($"event_date")

//validRecord.show(50,false)
val windowOverCond= Window.partitionBy($"id",$"model",$"imei").orderBy($"event_date")
val leadEventDate = validRecord.withColumn("event_date_lead",lead($"event_date",1).over(windowOverCond))

val userEventDetails = leadEventDate.filter($"event" === "android.intent.action.ACTION_POWER_CONNECTED").filter($"event_date_lead".isNotNull)

val batteryChargeDuration = userEventDetails.withColumn("charge_duration",(unix_timestamp($"event_date_lead") - unix_timestamp($"event_date")))

val batteryChargeStats = batteryChargeDuration.filter($"charge_duration" >= 120)
                                              .groupBy("id","model","imei")
                                              .agg(count($"charge_duration").alias("battery_charge_count")
                                              ,mean($"charge_duration").alias("battery_charge_avg_time").cast("integer"))



batteryChargeStats.show(5000,false)
batteryChargeStats.printSchema()

}
