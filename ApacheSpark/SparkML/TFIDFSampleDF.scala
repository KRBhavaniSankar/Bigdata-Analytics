package com.spark.mlib

import org.apache.spark.ml.feature.{Tokenizer,HashingTF,IDF}
import org.apache.spark.sql.SparkSession

object TfIDFDataFrame extends App {


  val spark = SparkSession.builder()
    .master("local[*]")
    .appName(s"Ranking Hints: ${args.mkString(",")}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val sentenceData = spark.createDataFrame(Seq(
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
  )).toDF("label", "sentence")
      .select("label","sentence")

  //sentenceData.show(10,false)       //Hi I heard about Spark


  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val wordsData = tokenizer.transform(sentenceData)
  //wordsData.show(false)     //[hi, i, heard, about, spark]

  val hashingTF = new HashingTF()
    .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

  val featurizedData = hashingTF.transform(wordsData)

  // alternatively, CountVectorizer can also be used to get term frequency vectors

  //featurizedData.show(false)        //(20,[0,5,9,17],[1.0,1.0,1.0,2.0])
  /*

   */
  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)

  val rescaledData = idfModel.transform(featurizedData)
  rescaledData.select("label", "features")
    .show(false)

  /*
  +-----+----------------------------------------------------------------------------------------------------------------------+
|label|features                                                                                                              |
+-----+----------------------------------------------------------------------------------------------------------------------+
|0.0  |(20,[0,5,9,17],[0.6931471805599453,0.6931471805599453,0.28768207245178085,1.3862943611198906])                        |
|0.0  |(20,[2,7,9,13,15],[0.6931471805599453,0.6931471805599453,0.8630462173553426,0.28768207245178085,0.28768207245178085]) |
|1.0  |(20,[4,6,13,15,18],[0.6931471805599453,0.6931471805599453,0.28768207245178085,0.28768207245178085,0.6931471805599453])|
+-----+----------------------------------------------------------------------------------------------------------------------+

   */

}

