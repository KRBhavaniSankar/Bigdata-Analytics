import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object UserEventLogsEDA extends App {


  val DAT_userEventPath = args(0)
  val DAT_NOT_UserEventLogPath = args(1)
  val DAT_NOT_outputpath = args(2)
  val DAT_outputpath= args(3)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(s"AppUseLiteracyEDA: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val DATDF = spark.read.option("mergeSchema","true").parquet(DAT_userEventPath)
  val DATNotDF= spark.read.option("mergeSchema","true").parquet(DAT_NOT_UserEventLogPath)


  val validRecord = DATDF.select("id","model","event","event_date","imei")
    .filter($"event_date".between("2017-03-29 00:00:00","2018-10-31 23:59:59"))
    .sort($"event_date")

  val validRecordDatNotDF = DATNotDF.select("id","model","event","event_date","imei")
    .filter($"event_date".between("2017-03-29 00:00:00","2018-10-31 23:59:59"))
    .sort($"event_date")

  val windowOverCond= Window.partitionBy($"id",$"model",$"imei").orderBy($"event_date")

  val leadEventDate = validRecord.withColumn("event_date_lead",lead($"event_date",1).over(windowOverCond))
  val leadEventdate_DAT_NOT= validRecordDatNotDF.withColumn("event_date_lead",lead($"event_date",1).over(windowOverCond))

  val userEventDetails = leadEventDate.filter($"event" === "android.intent.action.ACTION_POWER_CONNECTED").filter($"event_date_lead".isNotNull)
  val userEventDATNOTDetails = leadEventdate_DAT_NOT.filter($"event" === "android.intent.action.ACTION_POWER_CONNECTED").filter($"event_date_lead".isNotNull)

  val batteryChargeDuration = userEventDetails.withColumn("charge_duration",(unix_timestamp($"event_date_lead") - unix_timestamp($"event_date")))
  val batteryChargeDuration_DATNOT= userEventDATNOTDetails.withColumn("charge_duration",(unix_timestamp($"event_date_lead") - unix_timestamp($"event_date")))

  val batteryChargeStats = batteryChargeDuration.filter($"charge_duration" >= 120)
                                                .groupBy("id","model","imei")
                                                .agg(count($"charge_duration").alias("battery_charge_count")
                                                  ,round(mean($"charge_duration")).alias("battery_charge_avg_time")
                                                ,sum($"charge_duration").alias("battery_charge_sum"))

  val batteryChargeStats_DAT_NOT= batteryChargeDuration_DATNOT.filter($"charge_duration" >= 120)
                                                .groupBy("id","model","imei")
                                                .agg(count($"charge_duration").alias("battery_charge_count")
                                                ,round(mean($"charge_duration")).alias("battery_charge_avg_time")
                                                ,sum($"charge_duration").alias("battery_charge_sum"))

  batteryChargeStats_DAT_NOT.coalesce(1).write.option("header","true").mode("overwrite").csv(DAT_NOT_outputpath)

  batteryChargeStats.coalesce(1).write.option("header","true").mode("overwrite").csv(DAT_outputpath)

}
