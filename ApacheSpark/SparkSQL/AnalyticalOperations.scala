
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType


object TOKAppUseFeatureGen20200122 extends App {

  val appResLogs = args(0)
  val newTOKPath = args(1)
  val featuresOutputPath = args(2)

  val spark = SparkSession.builder()
    .appName(s"TOKAppLogsFeaturePreparation : ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val engColumns = List("factory_time","factory_imei","repair_process2","repairType","case_id","device_model","shop_region","shop_prefecture")
  val df = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .option("timestampFormat","yyyy/MM/dd HH:mm:ss")
    .csv(newTOKPath)
    .select("故障受付日時","故障機製造番号","修理工程２","故障受付内容１","case_id","故障機商品名","故障受付支社","支店")
    .toDF(engColumns: _*)
    .select($"factory_time".as("factory_timestamp"),regexp_replace($"factory_imei","'","").as("factory_imei").cast(LongType),regexp_replace($"repair_process2","ＴＯＫ*＊?","TOK").as("repair_process2"),$"repairType",$"case_id",$"device_model",$"shop_region",$"shop_prefecture")

  val appUseLogsDF = spark.read.parquet(appResLogs)

  val lastActivtyDF = appUseLogsDF
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id")
    .agg(max($"event_date").as("last_activity"))
    .withColumn("5days_before",$"last_activity"- expr("INTERVAL 5 DAYS"))

  appUseLogsDF.createOrReplaceTempView("app_logs")

  lastActivtyDF.createOrReplaceTempView("last_activity_ts")

  val query ="""
            SELECT A.last_activity,A.5days_before,B.*
            FROM last_activity_ts A INNER JOIN app_logs B
            ON A.factory_imei = B.factory_imei AND A.factory_timestamp = B.factory_timestamp AND A.case_id= B.case_id
            WHERE B.event_date between A.5days_before and A.last_activity
            """
  var finalLogsDF = spark.sql(query).distinct()

  val window = Window.partitionBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before").orderBy("event_date")

  val featureDF =finalLogsDF
    .distinct
    .withColumn("lead_time",lead($"event_date",1).over(window))
    .withColumn("lead_time_diff", (unix_timestamp($"lead_time","yyyy-MM-dd HH:mm:ss") - unix_timestamp($"event_date","yyyy-MM-dd HH:mm:ss")).cast(IntegerType))
    .withColumn("10min_session_gap",when($"lead_time_diff" >= 600 ,1).otherwise(0))
    .withColumn("30min_session_gap",when($"lead_time_diff" >= 1800 ,1).otherwise(0))


  val activityDF = featureDF
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before")
    .agg(sum($"10min_session_gap").as("usage_count_10min"),sum($"30min_session_gap").as("usage_count_30min"))


  val activityUsageDF = featureDF
    .filter($"30min_session_gap"===1)
    .withColumn("lead_event_started_time",lead($"event_date",1).over(window))
    .withColumn("active_usage_inSec", (unix_timestamp($"lead_event_started_time","yyyy-MM-dd HH:mm:ss") - unix_timestamp($"lead_time","yyyy-MM-dd HH:mm:ss")))
    .filter($"lead_time".isNotNull)

  activityUsageDF.createOrReplaceTempView("usage_df")

  val query1 ="""
            SELECT factory_imei,factory_timestamp,repair_process2,repairType,case_id,last_activity,5days_before,percentile_approx(active_usage_inSec,0.25) AS percentile25,percentile_approx(active_usage_inSec,0.5) AS percentile50,percentile_approx(active_usage_inSec,0.75) AS percentile75,max(active_usage_inSec) AS longest_usage,stddev(active_usage_inSec) AS usage_range_std_dev,mean(active_usage_inSec) AS mean_active_usage
            FROM usage_df
            GROUP BY factory_imei,factory_timestamp,repair_process2,repairType,case_id,last_activity,5days_before
        """
  val percentileDF = spark.sql(query1)

  val activityUsageStatsDF = activityDF.join(percentileDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"left_outer")

  val rankWindow = Window.partitionBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before").orderBy(desc("total_package_count"))

  val topUsedPackages = finalLogsDF
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","package_name")
    .agg(count("*").as("total_package_count"))
    .withColumn("rank",rank.over(rankWindow))
    .sort("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","rank")
    .where($"rank" <= 3)
    .drop($"package_name")

  val aggDF = finalLogsDF
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before")
    .agg(count($"package_name").as("activity_count"),size(collect_set($"package_name")).as("pkg_unique_count"))
    .withColumn("app_variety",($"pkg_unique_count"/$"activity_count"))


  val aggJoinDF = aggDF
    .join(topUsedPackages,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .withColumn("perc",($"total_package_count"/$"activity_count"))
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","activity_count","pkg_unique_count","app_variety")
    .agg(collect_list($"total_package_count").as("max_pkg_count"),collect_list($"perc").as("max_pkg_ratio"),sum($"perc").as("top3_pkg_ratio"))
    .select($"factory_imei",$"factory_timestamp",$"repair_process2",$"repairType",$"case_id",$"last_activity",$"5days_before",$"activity_count",$"pkg_unique_count",$"app_variety",$"max_pkg_count".getItem(0).as("max_pkg_count1"),$"max_pkg_count".getItem(1).as("max_pkg_count2"),$"max_pkg_count".getItem(2).as("max_pkg_count3"),$"max_pkg_ratio".getItem(0).as("max_pkg_ratio1"),$"max_pkg_ratio".getItem(1).as("max_pkg_ratio2"),$"max_pkg_ratio".getItem(2).as("max_pkg_ratio3"),$"top3_pkg_ratio")

  def analyticalDF(df:DataFrame, col:String, fill_ration:Long, alias:String):DataFrame ={

    df.createOrReplaceTempView("cal_df")
    val query =s"""
            SELECT factory_imei,factory_timestamp,repair_process2,repairType,case_id,last_activity,5days_before,percentile_approx(${col},0.25) AS percentile25_${alias},
            percentile_approx(${col},0.5) AS percentile50_${alias},percentile_approx(${col},0.75) AS percentile75_${alias},max(${col}) AS max_usage_${alias},stddev(${col}) AS std_dev_${alias},mean(${col}) AS ${alias}_mean_${col},sum(${col})/${fill_ration} AS fill_ration_${alias}
            FROM cal_df
            GROUP BY factory_imei,factory_timestamp,repair_process2,repairType,case_id,last_activity,5days_before
        """
    val newDF = spark.sql(query)
    newDF
  }

  val halfMinDF = finalLogsDF
    .select("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","package_name","event_date")
    .distinct
    .withColumn("day",to_date($"event_date"))
    .withColumn("hour",hour($"event_date"))
    .withColumn("minute",minute($"event_date"))
    .withColumn("second_bin",(second($"event_date")/30).cast(IntegerType))
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","day","hour","minute","second_bin")
    .agg(count($"package_name").as("30sec_count"))

  val oneMinDF = finalLogsDF
    .select("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","package_name","event_date")
    .distinct
    .withColumn("day",to_date($"event_date"))
    .withColumn("hour",hour($"event_date"))
    .withColumn("minute",(minute($"event_date")).cast(IntegerType))
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","day","hour","minute")
    .agg(count($"package_name").as("minute_count"))


  val tenMinDF = finalLogsDF
    .select("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","package_name","event_date")
    .distinct
    .withColumn("day",to_date($"event_date"))
    .withColumn("hour",hour($"event_date"))
    .withColumn("10_minute",(minute($"event_date")/10).cast(IntegerType))
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","day","hour","10_minute")
    .agg(count(lit(1)).as("minute_count"))

  val thirtyMinDF = finalLogsDF
    .select("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","package_name","event_date")
    .distinct
    .withColumn("day",to_date($"event_date"))
    .withColumn("hour",hour($"event_date"))
    .withColumn("30_minute",(minute($"event_date")/30).cast(IntegerType))
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","day","hour","30_minute")
    .agg(count(lit(1)).as("minute_count"))

  val sixtyMinDF = finalLogsDF
    .select("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","package_name","event_date")
    .distinct
    .withColumn("day",to_date($"event_date"))
    .withColumn("hour",hour($"event_date"))
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","day","hour")
    .agg(count(lit(1)).as("minute_count"))


  val finalHalfMinDF = analyticalDF(halfMinDF,"30sec_count",14400,"30sec")
  val finalOneMinDF = analyticalDF(oneMinDF,"minute_count",7200,"1min")
  val finalTenMinDF = analyticalDF(tenMinDF,"minute_count",720,"10min")
  val finalThirtyMinDF = analyticalDF(thirtyMinDF,"minute_count",240,"30min")
  val finalSixtyMinDF = analyticalDF(sixtyMinDF,"minute_count",120,"60min")

  val packageUsageAggDF= finalHalfMinDF
    .join(finalOneMinDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .join(finalTenMinDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .join(finalThirtyMinDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .join(finalSixtyMinDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")

  val packageNamesListDF = finalLogsDF
    .select("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","package_name")
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before")
    .agg(collect_list($"package_name").as("package_names_list"))
    .withColumn("package_names",concat_ws(",",$"package_names_list"))
    .drop($"package_names_list")

  val classNameListDF = finalLogsDF
    .groupBy("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before")
    .agg(collect_list($"class_name").as("class_name_list"))
    .withColumn("class_names",concat_ws(",",$"class_name_list"))
    .drop($"class_name_list")


  val finalJoinDF =aggJoinDF
    .join(activityUsageStatsDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .join(packageUsageAggDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .join(packageNamesListDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .join(classNameListDF,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before"),"inner")
    .join(df,Seq("factory_imei","factory_timestamp","repair_process2","repairType","case_id"),"inner")


  println("------------ final logs schema -------------")

  finalJoinDF.printSchema()

  finalJoinDF
    .write
    .mode(SaveMode.Overwrite)
    .parquet(featuresOutputPath)

  println("output write successfully completed !")

  println(s"Total number of cases : ${finalJoinDF.count()}")
  println(finalJoinDF.select("factory_imei","factory_timestamp","repair_process2","repairType","case_id","last_activity","5days_before","device_model").distinct.count)


  spark.stop()
  println("spark  stopped !")
  println("-----END-------")

}
