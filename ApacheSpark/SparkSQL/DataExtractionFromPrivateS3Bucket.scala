import org.apache.spark.sql.SparkSession
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat

object AIEventLogData extends App {

  val accessKey = args(0)
  val accessSecretKey = args(1)
  val s3Path = args(2)
  val collectionBucket = args(3)
  val backupBucket = args(4)
  val startDate = args(5)
  val endDate= args(6)
  val outputPath = args(7)

  val logType = "ai_event_log"

  val originalFormat = DateTimeFormat.forPattern("yyyyMMdd")
  val newStartDate = originalFormat.parseDateTime(startDate)
  val newEndDate= originalFormat.parseDateTime(endDate)

  val durationDays = Days.daysBetween(newStartDate,newEndDate).getDays()

  val tempS3Path = (0 to durationDays).map(date => {
    val newDate = newStartDate.plusDays(date)
    val newTargetDate = newDate.toString("yyyyMMdd")
    val targetMonth = newDate.toString("yyyyMM")
    s"${targetMonth}/${newTargetDate}/t_${logType}_${newTargetDate}_*"
  })

  val finalS3Path = tempS3Path.mkString(",")
  val collectionS3Path = s"${s3Path}/${collectionBucket}/{${finalS3Path}}"
  val spark = SparkSession.builder()
    .appName(s"Copy Data : ${args.mkString(", ")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKey)
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", accessSecretKey)

  import spark.implicits._

  val aiEventDf = spark.read
    .option("header","false")
    .option("inferSchema","true")
    .option("timestampFormat","yyyy/MM/dd HH:mm:ss")
    .csv(collectionS3Path)
    .distinct()

  aiEventDf.write.parquet(outputPath)
  spark.stop()
  println("------------END--------------------")


}
