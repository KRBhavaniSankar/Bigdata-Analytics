import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RankingHints extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(s"AppUseLiteracyEDA: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._


  val path = args(0)
  val samplePath = s"${path}/part-00000-8f8c066e-7e3d-45a7-8e92-e99a28753070-c000.csv"

  val outputPath = args(1)

  val pairHintsCountsOutput =s"${outputPath}/pairHintCounts"
  val hintCountsOutput= s"${outputPath}/hintCounts"


  val aiEventDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv(path)
    .distinct()

  val filterDF = aiEventDF.filter($"eventRoot"===0 && $"eventType"===3 && $"eventDetail"===1)

  val rankingCatDF = filterDF.select("result")
    .withColumn("newRes",regexp_replace($"result", "[\\[?\\]$]", ""))//.cast(ArrayType))


  val splitDF = rankingCatDF.withColumn("hints",split($"newRes","_"))
  val explodeDF = splitDF.select("result","hints").withColumn("hint",explode($"hints"))

  val pairHintCounts = explodeDF.select("result")
    .groupBy($"result")
    .agg(count($"result").as("pairHintCounts"))
    .sort($"pairHintCounts".desc)

  pairHintCounts.coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .csv(pairHintsCountsOutput)


  val hintCountsDF = explodeDF.select("hint")
    .groupBy($"hint")
    .agg(count($"hint").as("hint_count"))
    .sort($"hint_count".desc)

  hintCountsDF.coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .csv(hintCountsOutput)


}
