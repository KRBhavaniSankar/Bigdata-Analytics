import org.apache.spark.sql.SparkSession

object AIEventValidation extends App {


  val aiEventPath = args(0)

  val spark = SparkSession.builder()
    .appName(s"AIEventLog : ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val aiEventDF = spark.read.option("mergeSchema","true").parquet(aiEventPath)

  val aiEventDef = aiEventDF.selectExpr("_c0 as Index","_c1 as _id","_c2 as IMEI","_c3 as model","_c4 as os_version","_c5 as helpID","_c6 as eventTime","_c7 as eventRoot","_c8 as eventType","_c9 as eventDetail","_c10 result")

  val filterDF = aiEventDef.filter(($"eventRoot") === 0 && $"eventType"===3 && $"eventDetail"===2)
  //filterDF.show(100,false)
  //println(filterDF.distinct().count())      //5259560

  val distinctDF = filterDF.select("_id","IMEI")
      .filter($"_id".isNotNull && $"IMEI".isNotNull)
      .distinct()

  distinctDF.show(1000,false)

  println(s"total distinct reocrds : ${distinctDF.distinct().count()}\ndistinct ID :${distinctDF.select("_id").distinct().count()}\ndistinct IMEI : ${distinctDF.select("IMEI").distinct().count()}")

  spark.stop()
}
