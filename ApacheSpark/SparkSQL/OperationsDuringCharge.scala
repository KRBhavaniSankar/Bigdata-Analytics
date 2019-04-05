import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object OperationsDuringCharge extends App {
val userEventPath = args(0)
val appUseLiteracyLogs = args(1)
val userEventsStatsOutputPath = args(2)

val spark = SparkSession.builder()
  .appName(s"UserEventStats: ${args.mkString(",")}")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

import spark.implicits._

val DATDF = spark.read
  .option("mergeSchema","true")
  .parquet(userEventPath)
  .select("id","event","event_date")
  .sort($"event_date")

val windowOverCond= Window.partitionBy($"id").orderBy($"event_date")

val leadEventDate = DATDF.withColumn("event_date_end",lead($"event_date",1).over(windowOverCond))
  .withColumn("event_lead",lead($"event",1).over(windowOverCond))

val colsToRemove = Seq("event", "event_lead")

val filterDF = leadEventDate.filter(($"event"==="android.intent.action.ACTION_POWER_CONNECTED") && not(($"event"===$"event_lead"))).drop(colsToRemove:_*)

val appUseLiteracyDF = spark.read
  .option("mergeSchema","true")
  .parquet(appUseLiteracyLogs)
  .select("imei","package_name","event_date")

filterDF.createOrReplaceTempView("user_event_df")
appUseLiteracyDF.createOrReplaceTempView("appuse_data")

val query ="""
          SELECT A.*,B.package_name
          FROM user_event_df A INNER JOIN appuse_data B
          ON A.id = B.imei
          WHERE B.event_date between A.event_date and A.event_date_end
          """
val appUseEventsDf = spark.sql(query)

val timeFrameWindow = Window.partitionBy("id","event_date","event_date_end")

val totalAggAppCounts = appUseEventsDf.withColumn("total_app_counts",count("event_date_end").over(timeFrameWindow))

val aggAppCounts = totalAggAppCounts.groupBy("id","event_date","event_date_end","package_name","total_app_counts")
  .agg(count($"package_name").alias("used_count"))

val batteryChargeDuration = aggAppCounts.withColumn("charge_duration_min",((unix_timestamp($"event_date_end") - unix_timestamp($"event_date"))/60).cast(IntegerType))
  .drop(Seq("event_date","event_date_end"):_*)
  .sort($"charge_duration_min".desc,$"used_count".desc)

batteryChargeDuration.show(100,false)
batteryChargeDuration.printSchema()

/*  batteryChargeDuration.coalesce(1)
  .write
  .mode(saveMode = "overwrite")
  .option("header","true")
  .csv(userEventsStatsOutputPath)*/
}
