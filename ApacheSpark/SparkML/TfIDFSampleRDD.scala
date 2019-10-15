package com.spark.mlib

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.IDF

object TfIDFSampleRDD extends App {

  //TODO replace with path specific to your machine
  val file = "file:///Users/bhavani.sankar/Desktop/Bhavani/spark-2.4.0/README.md"
  val spConfig = (new SparkConf()).setMaster("local[*]").setAppName("TFIDFSample")
  val sc = new SparkContext(spConfig)
  val documents = sc.textFile(file)
    .map(_.split(" ").toSeq)

  //documents.take(20).foreach(println(_))
  //print("Documents Size:" + documents.count)


  val hashingTF = new HashingTF
  val tf = hashingTF.transform(documents)
  for(tf_ <- tf) {
    println(s"$tf_")
  }
  tf.cache()
  val idf = new IDF().fit(tf)
  val tfidf = idf.transform(tf)
  println("tfidf size : " + tfidf.count)
  for(tfidf_ <- tfidf) {
    println(s"$tfidf_")
  }

}
