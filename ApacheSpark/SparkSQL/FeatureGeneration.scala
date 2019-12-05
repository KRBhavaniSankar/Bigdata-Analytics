package com.bhavani

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object FeatureGeneration extends App {
  
  val troubleUsersPath = args(0)
  val outputPath = args(1)

  val spark = SparkSession.builder()
    .appName(s"AppUseLogsFeatureGeneration : ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val troubleUsersDF = spark.read.option("mergeSchema", "true").parquet(troubleUsersPath).distinct
  val eventDateWindow = Window.partitionBy("CASE_ID","TIMESTAMP","imei").orderBy("event_date")

  val featureDF =troubleUsersDF
    .select("CASE_ID","TIMESTAMP","imei","package_name","event_date")
    .distinct
    .withColumn("lead_time",lead($"event_date",1).over(eventDateWindow))
    .withColumn("lead_time_diff", (unix_timestamp($"lead_time","yyyy-MM-dd HH:mm:ss") - unix_timestamp($"event_date","yyyy-MM-dd HH:mm:ss")).cast(IntegerType))
    .withColumn("10min_session_gap",when($"lead_time_diff" >= 600 ,1).otherwise(0))
    .withColumn("30min_session_gap",when($"lead_time_diff" >= 1800 ,1).otherwise(0))

  val activityDF = featureDF
    .groupBy("CASE_ID","TIMESTAMP","imei")
    .agg(sum($"10min_session_gap").as("usage_count_10min"),sum($"30min_session_gap").as("usage_count_30min"))

  val activityUsageDF = featureDF
    .filter($"10min_session_gap"===1)
    .withColumn("lead_event_started_time",lead($"event_date",1).over(eventDateWindow))
    .withColumn("active_usage_inSec", (unix_timestamp($"lead_event_started_time","yyyy-MM-dd HH:mm:ss") - unix_timestamp($"lead_time","yyyy-MM-dd HH:mm:ss")))
    .groupBy("CASE_ID","TIMESTAMP","imei")
    .agg(max($"active_usage_inSec").as("longest_usage"))

  val activityUsageStatsDF = activityDF.join(activityUsageDF,Seq("CASE_ID","TIMESTAMP","imei"),"left_outer")
  val rankWindow = Window.partitionBy("CASE_ID","TIMESTAMP","2hours_before","min_event_date","1hour_before","imei").orderBy(desc("total_package_count"))

  val topUsedPackages = troubleUsersDF
    .groupBy("CASE_ID","TIMESTAMP","2hours_before","min_event_date","1hour_before","imei","package_name")
    .agg(count("*").as("total_package_count"))
    .withColumn("rank",rank.over(rankWindow))
    .sort($"imei",$"TIMESTAMP",$"rank")
    .where($"rank" <= 3)
    .drop($"package_name")

  val aggDF = troubleUsersDF
    .groupBy("CASE_ID","TIMESTAMP","imei")
    .agg(count($"package_name").as("activity_count"),size(collect_set($"package_name")).as("pkg_unique_count"))
    .withColumn("app_variety",($"pkg_unique_count"/$"activity_count"))

  val aggJoinDF = aggDF
    .join(topUsedPackages,Seq("CASE_ID","TIMESTAMP","imei"),"inner")
    .withColumn("perc",($"total_package_count"/$"activity_count"))
    .groupBy("CASE_ID","TIMESTAMP","imei","activity_count","pkg_unique_count","app_variety")
    .agg(collect_list($"total_package_count").as("max_pkg_count"),collect_list($"perc").as("max_pkg_ratio"),sum($"perc").as("top3_pkg_ratio"))
    .select($"CASE_ID",$"TIMESTAMP",$"imei",$"activity_count",$"pkg_unique_count",$"app_variety",$"max_pkg_count".getItem(0).as("max_pkg_count1"),$"max_pkg_count".getItem(1).as("max_pkg_count2"),$"max_pkg_count".getItem(2).as("max_pkg_count3"),$"max_pkg_ratio".getItem(0).as("max_pkg_ratio1"),$"max_pkg_ratio".getItem(1).as("max_pkg_ratio2"),$"max_pkg_ratio".getItem(2).as("max_pkg_ratio3"),$"top3_pkg_ratio")

  val oneMinDF = troubleUsersDF
    .select("CASE_ID","TIMESTAMP","imei","package_name","event_date")
    .distinct
    .withColumn("minute",minute($"event_date"))
    .groupBy("CASE_ID","TIMESTAMP","imei","minute")
    .agg(count(lit(1)).as("minute_count"))
    .groupBy("CASE_ID","TIMESTAMP","imei")
    .agg(max($"minute_count").as("max_count_1min"),mean($"minute_count").as("mean_count_1min"),(sum($"minute_count")/60).as("filled_ration_1min"))

  val fiveMinDF = troubleUsersDF
    .select("CASE_ID","TIMESTAMP","imei","package_name","event_date")
    .distinct
    .withColumn("minute_bin",(minute($"event_date")/5).cast(IntegerType))
    .groupBy("CASE_ID","TIMESTAMP","imei","minute_bin")
    .agg(count(lit(1)).as("minute_count"))
    .groupBy("CASE_ID","TIMESTAMP","imei")
    .agg(max($"minute_count").as("max_count_5min"),mean($"minute_count").as("mean_count_5min"),(sum($"minute_count")/12).as("filled_ration_5min"))

  val tenMinDF = troubleUsersDF
    .select("CASE_ID","TIMESTAMP","imei","package_name","event_date")
    .distinct
    .withColumn("minute_bin",(minute($"event_date")/10).cast(IntegerType))
    .groupBy("CASE_ID","TIMESTAMP","imei","minute_bin")
    .agg(count(lit(1)).as("minute_count"))
    .groupBy("CASE_ID","TIMESTAMP","imei")
    .agg(max($"minute_count").as("max_count_10min"),mean($"minute_count").as("mean_count_10min"),(sum($"minute_count")/6).as("filled_ration_10min"))

  val packageUsageAggDF= oneMinDF.join(fiveMinDF,Seq("CASE_ID","TIMESTAMP","imei"),"inner").join(tenMinDF,Seq("CASE_ID","TIMESTAMP","imei"),"inner")

  val packageNamesListDF = troubleUsersDF
    .select("CASE_ID","TIMESTAMP","imei","package_name")
    .distinct
    .withColumn("mod_package_name",regexp_replace($"package_name","\\.",""))
    .groupBy("CASE_ID","TIMESTAMP","imei")
    .agg(collect_list($"mod_package_name").as("package_names_list"))
    .withColumn("package_name",concat_ws(",",$"package_names_list"))
    .drop($"package_names_list")

  val finalFeaturesDF =aggJoinDF
    .join(activityUsageStatsDF,Seq("CASE_ID","TIMESTAMP","imei"),"inner")
    .join(packageUsageAggDF,Seq("CASE_ID","TIMESTAMP","imei"),"inner")
    .join(packageNamesListDF,Seq("CASE_ID","TIMESTAMP","imei"),"inner")

  finalFeaturesDF.show(10,false)

  finalFeaturesDF.printSchema()

  spark.stop()
}

