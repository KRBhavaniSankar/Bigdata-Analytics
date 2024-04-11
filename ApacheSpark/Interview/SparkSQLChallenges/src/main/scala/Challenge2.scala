package com.bhavani.interview.preparation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}


object Challenge2 extends App{

  private val hostName = "localhost"
  private val postgresDBType: String = "postgresql"
  private val postgresDBName: String = "postgres"
  private val postgresPort: String = "5432"
  private val postgresDriverName: String = "org.postgresql.Driver"
  private val postgresUserName: String = "postgres"
  private val postgresPassword: String = ""
  private val postgresTrailsTableName: String = "trails"
  private val postgresMountaiTableName: String = "mountain_huts"

  val spark = SparkSession
    .builder()
    .appName("Challenge2")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")


  val trailsDF = ConnectPostgresDB.createPostgresDF(spark, postgresDBType, postgresDBName, hostName, postgresPort, postgresDriverName, postgresUserName, postgresPassword, postgresTrailsTableName)

  //trailsDF.printSchema()
  //trailsDF.show(10,truncate =false)

  val mountainsDF = ConnectPostgresDB.createPostgresDF(spark, postgresDBType, postgresDBName, hostName, postgresPort, postgresDriverName, postgresUserName, postgresPassword, postgresMountaiTableName)

  //mountainsDF.printSchema()
  //mountainsDF.show(10,truncate = false)

  val joinDF1 = mountainsDF
    .join(trailsDF, $"id" === $"hut1","inner")
    .select($"hut1".as("start_hut"),$"name".as("start_hut_name"),$"altitude".as("start_hut_altitude"),$"hut2".as("end_hut"))
  //joinDF1.show(10,truncate = false)

  val joinDF2 = mountainsDF
    .join(trailsDF, $"id" === $"hut2","inner")
    .select($"hut2".as("end_hut"),$"name".as("end_hut_name"),$"altitude".as("end_hut_altitude"))
  //joinDF2.show(10,truncate = false)


  val finalJoinDF = joinDF1
    .join(joinDF2,Seq("end_hut"),"inner")
    .distinct()
    .withColumn("altitude_lag",when($"start_hut_altitude" > $"end_hut_altitude",1).otherwise(0))

  val prepDF = finalJoinDF
    .select(
      when($"altitude_lag" === 1, $"start_hut").otherwise($"end_hut").as("start_hut"),
      when($"altitude_lag" === 1,$"start_hut_name").otherwise($"end_hut_name").as("start_hut_name"),
      when($"altitude_lag" === 1,$"end_hut").otherwise($"start_hut").as("end_hut"),
      when($"altitude_lag" === 1,$"end_hut_name").otherwise($"start_hut_name").as("end_hut_name")
    )

  //finalJoinDF.show(20,false)
  prepDF.show(20,false)

  prepDF.createOrReplaceTempView("temp1")
  prepDF.createOrReplaceTempView("temp2")

  val query ="""
      |select t1.start_hut_name as start_hut_name,t1.end_hut_name as mid_hut_name,t2.end_hut_name as end_hut_name
      |from temp1 t1 join temp2 t2 on t1.end_hut = t2.start_hut
      |""".stripMargin


  val resDF = prepDF.as("t1")
    .join(prepDF.as("t2"),$"t1.end_hut"===$"t2.start_hut")
    .select(
      $"t1.start_hut".as("start_hut"),
      $"t1.start_hut_name".as("start_hut_name"),
      $"t1.end_hut".as("mid_hut"),
      $"t1.end_hut_name".as("mid_hut_name"),
      $"t2.end_hut".as("end_hut"),
      $"t2.end_hut_name".as("mid_hut_name"),
    )
    .drop("start_hut","mid_hut","end_hut")

  val resDF2  = spark.sql(query)

  resDF.show(10,false)
  resDF2.show(10,false)






}
