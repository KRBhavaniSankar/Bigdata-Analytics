package com.bhavani.interview.preparation

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object ConnectPostgresDB {

  def createPostgresDF(spark:SparkSession,
                       dbType: String,
                       dbName: String,
                       hostName: String,
                       port: String,
                       driverName: String,
                       userName: String,
                       password: String,
                       tableName: String) : DataFrame = {


    //jdbc connection details
    val jdbcURL = s"jdbc:$dbType://$hostName:$port/$dbName?zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8"

    println(s"connecting to $tableName view using $jdbcURL")

    val connectionProperties = new Properties()
    connectionProperties.put("driver", driverName)
    connectionProperties.put("user", userName)
    connectionProperties.put("password", password)

    //create df using jdbc connection properties
    val df = spark
      .read
      .jdbc(jdbcURL, tableName, connectionProperties)

    println(s"connected to $tableName view and created its df successfully !\n")

    df

  }

}
