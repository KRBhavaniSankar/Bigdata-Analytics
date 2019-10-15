import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.Months
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._

object TroubleUsersEDA extends App {

  val appUseLiteracyLogs =args(0)

  val imeiPath = args(1)

  val outputPath= args(2)

  val startDate = "20170501"
  val endDate = "20180731"

  val originalFormat = DateTimeFormat.forPattern("yyyyMMdd")
  val newStartDate = originalFormat.parseDateTime(startDate)

  val newEndDate = originalFormat.parseDateTime(endDate)

  val numberOfMonths = Months.monthsBetween(newStartDate,newEndDate).getMonths

  val tempS3Path = (0 to numberOfMonths).map(date => {
    val newMonth = newStartDate.plusMonths(date)
    val targetMonth = newMonth.toString("yyyy-MM")
    s"${targetMonth}"
  })

  val finalS3Path = s"${appUseLiteracyLogs}/{${tempS3Path.mkString(",")}}*/*"
  println(finalS3Path)

  val spark = SparkSession.builder()
    .appName(s"Trouble Users EDA: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val imeiDF = spark.read.option("header","true").option("inferSchema","true").csv(imeiPath)

  val hoursBeforeDF = imeiDF.withColumn("2hours_before", imeiDF("TIMESTAMP") - expr("INTERVAL 2 HOURS"))

  val appUseData = spark.read.parquet(finalS3Path)

  val selectedAppUseDF = appUseData.select("imei","event_date","package_name")

  hoursBeforeDF.createOrReplaceTempView("imei_data")
  selectedAppUseDF.createOrReplaceTempView("app_use_data")

  val query ="""
            SELECT A.*,B.event_date
            FROM imei_data A INNER JOIN app_use_data B
            ON A.IMEI = B.imei
            WHERE B.package_name ='com.rsupport.rs.activity.ntt'
            AND B.event_date between A.2hours_before and A.TIMESTAMP
            """
  val resultDF = spark.sql(query)

  val filterMinTime = resultDF.groupBy($"imei",$"TIMESTAMP",$"2hours_before")
    .agg(min($"event_date").alias("min_event_date"))


  filterMinTime.write
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .csv(outputPath)

  spark.stop()

}
