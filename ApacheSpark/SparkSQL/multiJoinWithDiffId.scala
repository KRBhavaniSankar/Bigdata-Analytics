package com.bhavani

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object ChatData extends App {

  val basePath = s"/Users/bhavani.sankar/Desktop/Proj/Tasks/chatdata"
  val callCenterPath = s"${basePath}/*.csv"
  val guestUsersFile01 = s"${basePath}/Users/guestUsers_20191007.txt"
  val selectFromReportData = s"${basePath}/Users/select_from_report_vraslineauthdata_201910071628.csv"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(s"ChatDataExtraction: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val chatDF = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .option("timestampFormat","yyyy/MM/dd HH:mm:ss")
    //.option("charset", "iso-8859-1")
    .option("charset","utf-16")
    .option("delimiter", "\t")
    //.option("escape", "\\")
    .option("multiline","true")
    //.option("encoding", "UTF-8")
    .csv(callCenterPath)
    .withColumnRenamed("送信者ID[msg.userId]","msg_userID")

    val selectedChatUsers = chatDF.select("msg_userID").distinct()
   // println(s"chat data :${chatDF.count()}\n chat users :${selectedChatUsers.count()}")

  val guestUsersDF = spark
    .read
    .json(guestUsersFile01)

    val selectGuestUsers = guestUsersDF
      .withColumn("snsUserIDParams_str",explode($"snsUserIdParams"))
      .select("userId","snsUserIDParams_str")
      .distinct()
    //println(guestUsersDF.count(),selectGuestUsers.count())

  val selectReportsDF = spark
    .read
    .option("header","true")
    .csv(selectFromReportData)

    val selectedReports =selectReportsDF.select("userid","linecontractnbr","linecontractid")

  //println(selectReportsDF.count(),selectedReports.distinct().count())

/*  chatDF.createOrReplaceTempView("chat_users")
  guestUsersDF.createOrReplaceTempView("guest_users")
  selectReportsDF.createOrReplaceTempView("selected_report_users")*/


  val joinDF = selectedChatUsers
    .join(selectGuestUsers,selectedChatUsers("msg_userID")===selectGuestUsers("userId"),"inner")
    .drop($"userId")
    .join(selectedReports,selectGuestUsers("snsUserIDParams_str")===selectedReports("userid"),"inner")

  //joinDF.show(10,false)
  //println(joinDF.distinct().count())
  val chatUsersOutputPath = s"${basePath}/chat_data_etl"
  joinDF
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .csv(chatUsersOutputPath)

}
