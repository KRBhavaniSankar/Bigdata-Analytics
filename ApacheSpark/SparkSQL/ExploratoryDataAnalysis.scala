
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AppUseEDA extends App{

  val dat_Users= args(0)
  val dat_NOT_users = args(1)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(s"AppUseLiteracyEDA: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val users_DAT = spark.read.option("header","true").option("inferSchema","true").csv(dat_Users)
  val users_DAT_NOT = spark.read.option("header","true").option("inferSchema","true").csv(dat_NOT_users)

  val usersAVGDAT = users_DAT.groupBy("package_name")
    .agg(sum($"agg_count").alias("DAT_total_count"))
    .withColumn("DAT_percentage",round($"DAT_total_count"/sum($"DAT_total_count").over() * 100))
    .sort($"DAT_percentage".desc)


  val usersAVGNotDAT = users_DAT_NOT.groupBy("package_name")
    .agg(sum($"agg_count").alias("DAT_NOT_total_count"))
    .withColumn("DAT_NOT_percentage",round($"DAT_NOT_total_count"/sum($"DAT_NOT_total_count").over() * 100))
    .sort($"DAT_NOT_percentage".desc)


  val allUsers = usersAVGDAT.join(usersAVGNotDAT,usersAVGDAT("package_name")===usersAVGNotDAT("package_name"),"full")
      .select(coalesce(usersAVGDAT("package_name"),usersAVGNotDAT("package_name")).alias("packagename"),usersAVGDAT("*"),usersAVGNotDAT("*"))
      .drop("package_name")
      .sort($"DAT_percentage".desc)

  //allUsers.show(1000,false)
  //allUsers.printSchema()

  allUsers.coalesce(1)
          .write
          .mode("overwrite")
          .option("header","true")
          .csv(args(2))

}
