package com.bhavani

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NetworkWordCount extends App{

  val spark = SparkSession
    .builder
    .appName("Network WordCount")
    .master("local[4]")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.shuffle.partitions",4)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to localhost:9999

  val linesDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  //linesDF.printSchema()

  // Split the lines into words
  //val words = lines.as[String].flatMap(_.split(" "))
  val wordsDF = linesDF.select(expr("explode(split(value, ' ')) as word"))

  // Generate running word count
  val wordCounts = wordsDF.groupBy("word").count()


  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .option("checkpointLocation","check-point-dir")
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()


}
