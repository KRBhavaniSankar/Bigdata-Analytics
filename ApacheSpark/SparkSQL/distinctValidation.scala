import org.apache.spark.sql.SparkSession

object AIEventValidation extends App {


  val aiEventPath = args(0)
  val timeRange = args(1)
  val outputPath  =args(2)

  val spark = SparkSession.builder()
    .appName(s"AIEventLogAnalysis : ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val aiEventDF = spark.read.option("mergeSchema","true").parquet(aiEventPath)

  val aiEventDef = aiEventDF.selectExpr("_c0 as Index","_c1 as _id","_c2 as IMEI","_c3 as model","_c4 as os_version","_c5 as helpID","_c6 as eventTime","_c7 as eventRoot","_c8 as eventType","_c9 as eventDetail","_c10 result")

  val filterDF = aiEventDef.filter(($"eventRoot") === 0 && $"eventType"===3 && $"eventDetail"===2)

  //filterDF.show(100,false)
  //println(filterDF.distinct().count())      //5259560

  val distinctDF = filterDF
      .filter($"_id".isNotNull)
      .select("_id","eventTime")
      .distinct()

  val minsBeforeDF = distinctDF.withColumn("3mins_before", $"eventTime" - expr(s"INTERVAL ${timeRange} MINUTE"))

  aiEventDef.createOrReplaceTempView("ai_event_df")
  minsBeforeDF.createOrReplaceTempView("mins_df")

  val query ="""
            SELECT B._id,B.model,B.os_version,B.helpID,B.eventTime,B.eventRoot,B.eventType,B.eventDetail,B.result
            FROM mins_df A RIGHT JOIN ai_event_df B
            ON A._id = B._id
            WHERE B.eventTime between A.3mins_before AND A.eventTime
            """
  val resultDF = spark.sql(query).sort($"_id",$"eventTime").distinct()

  resultDF.show(500,false)
  //resultDF.printSchema()

  println(s"total records count for ${timeRange} mins : ${resultDF.count()}")   //3 min - 32958845,2min - 28605432,1min-24399978,24132286

/*  resultDF.coalesce(30)
      .write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv(outputPath)*/

  spark.stop()}
