//package server.platform.wutiao
//
//import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
//import org.apache.spark
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassificationModel, RandomForestClassifier}
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.feature.{HashingTF, IDF, MinMaxScaler, Normalizer}
//import org.apache.spark.ml.linalg.Vectors
//import org.apache.spark.mllib.util.MLUtils
//import com.hankcs.hanlp.tokenizer.StandardTokenizer
//import org.apache.spark.ml.recommendation.ALS
//import org.apache.spark.mllib.feature.HashingTF
//
//import scala.collection.JavaConversions._
///**
//  * @program: customer_model_predict_age_sex
//  * @description: ${description}
//  * @author: maoyunlong
//  * @create: 2018-11-24 11:19
//  **/
//object Test {
////  ALS.train(ratings,10,maxIter = 10,lam)
//
//  //  spark.read.
////  val normalize = new Normalizer().setInputCol("xdata").setOutputCol("MaxMinout").setP(1.0)
////  val maxmin = new MinMaxScaler().setInputCol("xdata").setOutputCol("MaxMinout")
////  //RandomForestClassifier
////  val randomForset = new RandomForestClassifier().setFeaturesCol("MaxMinout").setLabelCol("Ydata").setPredictionCol("Predict")
////    .setFeatureSubsetStrategy("sqrt").setImpurity("entry").setMaxBins(20).setMaxDepth(15).setNumTrees(100)
//// val model_pipe = new Pipeline().setStages(Array(normalize,randomForset))
////  val model = model_pipe.fit(dataset)
////  model.write.overwrite().save("")
////  model.transform(dataset).createOrReplaceTempView("aa")
////  spark.sql("select Ydata,Predict from aa")
////  val evaluator = new MulticlassClassificationEvaluator().setPredictionCol("Predict").setLabelCol("Ydata")
////  val acc = evaluator.setMetricName("accuracy").evaluate(vaild_Prediction)
////  val precision = evaluator.setMetricName("weightedPrecision").evaluate(vaild_Prediction)
////  val weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(vaild_Prediction)
////  val f1 = evaluator.setMetricName("f1").evaluate(vaild_Prediction)
////
//val lr = new LogisticRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(1).setFeaturesCol("feature")
//  .setLabelCol("index").setPredictionCol("predict")
//def main(args: Array[String]): Unit ={
//  val a = "文件转为UTF-8编码及存储到一个文件"
//  val list = StandardTokenizer.segment(a)
//  CoreStopWordDictionary.apply(list)
//  list.map(x=>x.word.replaceAll(" ","")).toList
//
//  IDF
//
////  // 3. 求TF
////  println("calculating TF ...")
////  val hashingTF = new HashingTF()
////    .setInputCol("sentence_words").setOutputCol("rawFeatures").setNumFeatures(numFeatures)
////  val featurizedData = hashingTF.transform(docs)
////
////  // 4. 求IDF
////  println("calculating IDF ...")
////  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
////  val idfModel = idf.fit(featurizedData)
////  val rescaledData = idfModel.transform(featurizedData).cache()
//  import org.apache.spark.SparkConf
//  import org.apache.spark.SparkContext
//  import org.apache.spark.sql.SQLContext
//  import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
//  val sc = new SparkContext().master(local[*])
//  val sqlContext = new SQLContext(sc)
//  import sqlContext.implicits._
//  val sentenceData = sqlContext.createDataFrame(Seq(
//           (0, "I heard about Spark and I love Spark"),
//           (0, "I wish Java could use case classes"),
//           (1, "Logistic regression models are neat")
//           )).toDF("label", "sentence")
//  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//  val wordsData = tokenizer.transform(sentenceData)
//  val hashingTF = new HashingTF().
//           setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
//  val featurizedData = hashingTF.transform(wordsData)
//  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//  val idfModel = idf.fit(featurizedData)
//  val rescaledData = idfModel.transform(featurizedData)
//  rescaledData.select("features", "label").take(3).foreach(println)
//
//
//}
//
//}
