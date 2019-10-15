import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
object AIEventAppUseLiteracyLogs extends App {

  val spark = SparkSession.builder()
    .appName(s"AppUseLiteracyEDA: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val appUseEvents = args(0)

  val appUseLiteracyLogs =args(1)

  val startDate = args(2)
  val endDate = args(3)
  val packageAggCountoutputPath = args(4)

  println(packageAggCountoutputPath)
  val classAggCountOutputPath = args(5)

  println(classAggCountOutputPath)

  val originalFormat = DateTimeFormat.forPattern("yyyyMMdd")
  val newStartDate = originalFormat.parseDateTime(startDate)
  val newEndDate= originalFormat.parseDateTime(endDate)

  val durationDays = Days.daysBetween(newStartDate,newEndDate).getDays()

  val tempS3Path = (0 to durationDays).map(date => {
    val newDate = newStartDate.plusDays(date)
    val newTargetDate = newDate.toString("yyyy-MM-dd")
    s"${newTargetDate}/*"
  })

  val finalS3Path = tempS3Path.mkString(",")
  val appUseLogS3Path = s"${appUseLiteracyLogs}/{${finalS3Path}}"

  val aiEventDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv(appUseEvents)

  //println(s"before distinct : ${aiEventDF.count()}\n After distinct : ${aiEventDF.distinct().count()}")

  val fiveMinsBeforeDF =   aiEventDF
    .withColumn("2mins_before", $"eventTime" - expr("INTERVAL 2 MINUTE"))
    .withColumn("2mins_after", $"eventTime" + expr("INTERVAL 2 MINUTE"))

  val appUseLogDF = spark.read.option("mergerSchema","true").parquet(appUseLogS3Path)

  fiveMinsBeforeDF.createOrReplaceTempView("ai_data")

  appUseLogDF.createOrReplaceTempView("appuse_literacy_log")

  val query ="""
            SELECT A._id,A.eventTime,B.package_name,B.class_name
            FROM ai_data A INNER JOIN appuse_literacy_log B
            ON A._id = B.imei
            WHERE B.event_date between A.2mins_before AND 2mins_after
            """

  val filterAppUseLogs = spark.sql(query).distinct()

  filterAppUseLogs.cache()

  val packageAggCount = filterAppUseLogs.select("package_name")
      .groupBy($"package_name")
      .agg(count($"package_name").as("package_name_count"))

  val classAggCount = filterAppUseLogs.select("class_name")
      .groupBy($"class_name")
      .agg(count($"class_name").as("class_name_count"))

  packageAggCount
    .write
    .mode(SaveMode.Append)
    .csv(packageAggCountoutputPath)

  classAggCount
    .write
    .mode(SaveMode.Append)
    .csv(classAggCountOutputPath)

  spark.stop()

  }
