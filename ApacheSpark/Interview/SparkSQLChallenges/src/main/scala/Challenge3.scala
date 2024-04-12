package com.bhavani.interview.preparation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Challenge3 extends App{

  private val hostName = "localhost"
  private val postgresDBType: String = "postgresql"
  private val postgresDBName: String = "postgres"
  private val postgresPort: String = "5432"
  private val postgresDriverName: String = "org.postgresql.Driver"
  private val postgresUserName: String = "postgres"
  private val postgresPassword: String = ""
  private val TableName: String = "footer"


  val spark = SparkSession
    .builder()
    .appName("Challenge3")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val footerTableDF = ConnectPostgresDB.createPostgresDF(spark, postgresDBType, postgresDBName, hostName, postgresPort, postgresDriverName, postgresUserName, postgresPassword, TableName)

  //footerTableDF.printSchema()
  footerTableDF.show(10,truncate = false)
/*
  val footerWindow = Window.orderBy($"id".desc)

  val testDF = footerTableDF
    .withColumn("car",last($"car",ignoreNulls = true).over(footerWindow))
    .withColumn("length",last($"length",ignoreNulls = true).over(footerWindow))
    .withColumn("width",last($"width",ignoreNulls = true).over(footerWindow))
    .withColumn("height",last($"height",ignoreNulls = true).over(footerWindow))

  // Select only the last row (which has non-null values for all columns)
  //val lastNonNullRow = testDF.filter($"id" === testDF.agg(max($"id")).head.getLong(0))

  // Select rows with all non-null values (alternative to filtering by id)
  val lastNonNullRow = testDF.filter(col("car").isNotNull and col("length").isNotNull and col("width").isNotNull and col("height").isNotNull)*/

  // Assuming your dataframe is named "df"
  val lastNonNullRow = footerTableDF.agg(
    last($"car", ignoreNulls = true).as("car"),
    last($"length", ignoreNulls = true).as("length"),
    last($"width", ignoreNulls = true).as("width"),
    last($"height", ignoreNulls = true).as("height")
  ).select($"car", $"length", $"width", $"height")


  // Show the result
  lastNonNullRow.show(10,false)


  // Assuming your dataframe is named "df"
  val firstNonNullRow = footerTableDF.agg(
    first($"car", ignoreNulls = true).as("car"),
    first($"length", ignoreNulls = true).as("length"),
    first($"width", ignoreNulls = true).as("width"),
    first($"height", ignoreNulls = true).as("height")
  ).select($"car", $"length", $"width", $"height")

  firstNonNullRow.show(10,false)






}
