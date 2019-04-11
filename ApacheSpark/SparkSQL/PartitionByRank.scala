import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AppUseCoOccurance extends App {

  val appUseLiteracyLogs = args(0)
  val outputPath =  args(1)


  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(s"AppUseLiteracyEDA: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val DAT_Users = spark.read.option("mergeSchema","true").parquet(appUseLiteracyLogs).select("imei","package_name","event_date")

  //DAT_Users.show(50,false)
  //DAT_Users.printSchema()

  //val collectList = DAT_Users.groupBy("imei","event_date").agg(collect_list("package_name").alias("package_list"))

  //collectList.show(100,false)

  //println(DAT_Users.count())  //52323705

  //println(DAT_Users.filter($"").select("package_name").distinct().count())
  val aggPackageCount = DAT_Users.groupBy("imei","package_name").agg(count($"package_name").alias("package_count"))
  //aggPackageCount.show(1000,false)

  val w = Window.partitionBy($"imei").orderBy($"package_count".desc)
  val dfTop5PackageNames = aggPackageCount.withColumn("rn", row_number.over(w)).where($"rn" <= 5)//.drop($"rn")
  dfTop5PackageNames.write.mode(SaveMode.Overwrite).option("header","true").csv(outputPath)

  //println(dfTop5PackageNames.select("package_name").distinct.count())
  }
