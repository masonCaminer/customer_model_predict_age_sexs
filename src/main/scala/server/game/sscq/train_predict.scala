package server.game.sscq

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Row}
import server.game.sscq.PropertiesFactory._
import server.platform.xy.train_predictModel_xy_sex.{appName, num_core, num_excutor}
import utils.EvaluationAlgorithm
import utils.SparkGlobalSession.buildSparkSession

/**
  * @program: customer_model_predict_age_sex
  * @description: 训练模型
  * @author: maoyunlong
  * @create: 2018-09-21 11:08
  **/
object train_predict {
  /**
    * 训练
    *
    * @param trainRawData
    * @return
    */
  def fit_model_v2(trainRawData: DataFrame, genre: String) = {
    val Data = trainRawData.randomSplit(Array(0.8, 0.2), System.currentTimeMillis())
    val trainSet = Data(0)
    val vaildSet = Data(1)
    var isloadModel = false
    var subStrategy = "sqrt"
    var impurity = "entropy"
    var maxDepth = 6
    var numTrees = 20
    var maxBins = 10
    var PipeLineModelPath = ""
    if (genre == "sex") {
      subStrategy = "sqrt"
      impurity = "gini"
      maxDepth = 10
      numTrees = 30
      maxBins = 20
      println(s"train size is ${trainSet.count()}")
      println(s"label-0 size is :${trainSet.filter("Ydata=0").count()}")
      println(s"label-1 size is :${trainSet.filter("Ydata=1").count()}")
      println(s"label-2 size is :${trainSet.filter("Ydata=2").count()}")
      PipeLineModelPath = crossValidPredictModelSex_Path
    } else if (genre == "age") {
      subStrategy = "sqrt"
      impurity = "entropy"
      maxDepth = 15
      numTrees = 100
      maxBins = 20
      println(s"train size is ${trainSet.count()}")
      println(s"label-0 size is :${trainSet.filter("Ydata=0").count()}")
      println(s"label-1 size is :${trainSet.filter("Ydata=1").count()}")
      println(s"label-2 size is :${trainSet.filter("Ydata=2").count()}")
      println(s"label-3 size is :${trainSet.filter("Ydata=3").count()}")
      println(s"label-4 size is :${trainSet.filter("Ydata=4").count()}")
      println(s"label-5 size is :${trainSet.filter("Ydata=5").count()}")
      println(s"label-6 size is :${trainSet.filter("Ydata=6").count()}")
      println(s"label-7 size is :${trainSet.filter("Ydata=7").count()}")
      PipeLineModelPath = crossValidPredictModelAge_Path
    }
    // 标准化
    //    val scaler = new MinMaxScaler().setInputCol("Xdata").setOutputCol("MaxMinOut")
    //正则化
    val scaler = new Normalizer().setInputCol("Xdata").setOutputCol("MaxMinOut").setP(1.0)
    val randomForest = new RandomForestClassifier().setFeaturesCol("MaxMinOut").setLabelCol("Ydata")
      .setPredictionCol("Predict").setProbabilityCol("Prob").setRawPredictionCol("rawPredict")
      .setSeed(System.currentTimeMillis())
      .setFeatureSubsetStrategy(subStrategy)
      .setImpurity(impurity)
      .setMaxBins(maxBins).setMaxDepth(maxDepth).setNumTrees(numTrees)
    val model_pipe = new Pipeline().setStages(Array(scaler, randomForest))
    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForest.numTrees, Array(20, 40, 60, 80))
      .addGrid(randomForest.maxBins, Array(10, 40, 80, 130))
      .addGrid(randomForest.maxDepth, Array(3, 6, 9, 12))
      .build()
    if (genre == "sex") {
      val cv = new CrossValidator()
        .setEstimator(model_pipe)
        .setEvaluator(new BinaryClassificationEvaluator().setLabelCol("Ydata").setRawPredictionCol("Predict"))
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(2)
      val model = cv.fit(trainSet)
      model.write.overwrite().save(PipeLineModelPath)
      vaild_metric(trainSet, model, genre, true)
      vaild_metric(vaildSet, model, genre, true)
    } else if (genre == "age") {
      val cv = new CrossValidator()
        .setEstimator(model_pipe)
        .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("Ydata").setPredictionCol("Predict"))
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(3)
      val model = cv.fit(trainSet)
      model.write.overwrite().save(PipeLineModelPath)
      vaild_metric(trainSet, model, genre, true)
      vaild_metric(vaildSet, model, genre, true)
    }
  }

  /**
    * 训练
    *
    * @param trainRawData
    */
  def fit_model(trainRawData: DataFrame, genre: String) = {
    val Data = trainRawData.randomSplit(Array(0.8, 0.2), System.currentTimeMillis())
    val trainSet = Data(0)
    val vaildSet = Data(1)
    var isloadModel = false
    var subStrategy = "sqrt"
    var impurity = "entropy"
    var maxDepth = 6
    var numTrees = 20
    var maxBins = 10
    var randomForestModelPath = ""
    if (genre == "sex") {
      isloadModel = false
      subStrategy = "sqrt"
      impurity = "gini"
      maxDepth = 10
      numTrees = 30
      maxBins = 20
      println(s"train size is ${trainSet.count()}")
      println(s"vaildSet size is ${vaildSet.count()}")
      println(s"label-0 size is :${trainSet.filter("Ydata=0").count()}")
      println(s"label-1 size is :${trainSet.filter("Ydata=1").count()}")
      println(s"label-2 size is :${trainSet.filter("Ydata=2").count()}")
      randomForestModelPath = PredictModelSex_Path
    } else if (genre == "age") {
      isloadModel = false
      subStrategy = "sqrt"
      impurity = "entropy"
      maxDepth = 15
      numTrees = 100
      maxBins = 20
      println(s"train size is ${trainSet.count()}")
      println(s"vaildSet size is ${vaildSet.count()}")
      println(s"label-0 size is :${trainSet.filter("Ydata=0").count()}")
      println(s"label-1 size is :${trainSet.filter("Ydata=1").count()}")
      println(s"label-2 size is :${trainSet.filter("Ydata=2").count()}")
      println(s"label-3 size is :${trainSet.filter("Ydata=3").count()}")
      println(s"label-4 size is :${trainSet.filter("Ydata=4").count()}")
      println(s"label-5 size is :${trainSet.filter("Ydata=5").count()}")
      println(s"label-6 size is :${trainSet.filter("Ydata=6").count()}")
      println(s"label-7 size is :${trainSet.filter("Ydata=7").count()}")

      randomForestModelPath = PredictModelAge_Path
    }
    //正则化
    val normalizer = new Normalizer().setInputCol("Xdata").setOutputCol("MaxMinOut").setP(1.0)
    // 标准化
    //    val normalizer = new MinMaxScaler().setInputCol("Xdata").setOutputCol("MaxMinOut")
    val randomForest = new RandomForestClassifier().setFeaturesCol("MaxMinOut").setLabelCol("Ydata")
      .setPredictionCol("Predict").setProbabilityCol("Prob").setRawPredictionCol("rawPredict")
      .setSeed(System.currentTimeMillis())
      .setFeatureSubsetStrategy(subStrategy)
      .setImpurity(impurity)
      .setMaxBins(maxBins).setMaxDepth(maxDepth).setNumTrees(numTrees)
    val model_pipe = new Pipeline().setStages(Array(normalizer, randomForest))
    val model = model_pipe.fit(trainSet)
    if (!isloadModel) {
      model.write.overwrite().save(randomForestModelPath)
      println("全量评估")
      vaild_metric(trainRawData, model, genre, true)
      println("训练集评估")
      vaild_metric(trainSet, model, genre, true)
      println("测试集评估")
      vaild_metric(vaildSet, model, genre, true)
    }
  }

  /**
    * 加载模型地址
    * 交叉验证
    *
    * @param modelPath
    * @return
    */
  def loadPredictModelCrossValidatorModel(modelPath: String) = {
    CrossValidatorModel.load(modelPath)
  }

  /**
    * 加载模型地址
    * 随机森林
    *
    * @param modelPath
    * @return
    */
  def loadPredictModelRandomForestClassificationModel(modelPath: String) = {
    //    RandomForestClassificationModel.load(modelPath)
    PipelineModel.load(modelPath)
  }

  /**
    * 校验
    *
    * @param data
    * @param model
    * @param ispostivelabel
    * @tparam M
    */
  def vaild_metric[M <: Model[M]](data: DataFrame, model: Model[M], genre: String, ispostivelabel: Boolean = true) = {
    val vaild_Prediction = model.transform(data)
    vaild_Prediction.createOrReplaceTempView("vaild_Prediction")
    val spark = buildSparkSession(num_excutor, num_core, appName)
//    val aa = spark.sql(
//      s"""
//         |select Ydata,Predict,sum(Ydata),sum(Predict)
//         |from vaild_Prediction
//         |where Ydata = Predict
//         |group by Ydata,Predict
//      """.stripMargin)
//    aa.show()
    val metrics = EvaluationAlgorithm.getMetrics(vaild_Prediction.select("Ydata", "Predict"))
    if (genre == "age") {
      println("多分类评估")
      val accuracy = metrics.accuracy;
      println("accuracy", accuracy)
      val recall = (0 to 7).map(cat => (cat, metrics.precision(cat), metrics.recall(cat), metrics.weightedFMeasure
      (cat)))
      println("(label,precision,recall,FMeasure)")
      recall.foreach(println)
    } else if (genre == "sex") {
      println("手写二分类评估")
      binaryClassMetric(vaild_Prediction, "Ydata", "Predict", false)
      binaryClassMetric(vaild_Prediction, "Ydata", "Predict", true)

    }
    println("综合评估算法")
    val result = EvaluationAlgorithm.comprehensiveAssessment("Ydata", "Predict", vaild_Prediction)
    val acc = result._1
    val precision = result._2
    val weightedRecall = result._3
    val f1 = result._4
    println("acc", acc)
    println("precision", precision)
    println("weightedRecall", weightedRecall)
    println("f1", f1)
  }

  /**
    * 二分类评估
    *
    * @param data
    * @param labelCol
    * @param predictCol
    * @param ispostivelabel
    */
  def binaryClassMetric(data: DataFrame, labelCol: String, predictCol: String, ispostivelabel: Boolean = true) = {
    val (truelabel, falselabel) = if (ispostivelabel) (2.0, 1.0) else (1.0, 2.0)
    val x = data.select(labelCol, predictCol).rdd.map { case Row(label: Double, predict: Double) => (predict, label) }
      .aggregate(collection.mutable.ArrayBuffer(0.0, 0.0, 0.0, 0.0))((u, x) => {
        x match {
          //TP
          case (predict, label) if predict == truelabel && label == truelabel => {
            u(0) = u(0) + 1.0
          }
          //FP
          case (predict, label) if predict == truelabel && label == falselabel => {
            u(1) = u(1) + 1.0
          }
          //TP
          case (predict, label) if predict == falselabel && label == falselabel => {
            u(2) = u(2) + 1.0
          }
          //TP
          case (predict, label) if predict == falselabel && label == truelabel => {
            u(3) = u(3) + 1.0
          }
        }
        u
      },
        (u1, u2) => {
          u1.zip(u2).map(x => x._1 + x._2)
        })
    val TPNum = x(0)
    val FPNum = x(1)
    val TNNum = x(2)
    val FNNum = x(3)
    println(TPNum, FPNum, TNNum, FNNum)
    val AllNum = TPNum + FPNum + TNNum + FNNum
    val accNum = TPNum + TNNum
    val acc = accNum / AllNum
    val precision = TPNum / (TPNum + FPNum)
    val recall = TPNum / (TPNum + FNNum)
    val f1 = 2 * TPNum / (2 * TPNum + FPNum + FNNum)
    println(s"data ${if (ispostivelabel) "postive" else "negative"} AllNumber is $AllNum and acc is $acc\n" +
      s"precision:$precision\n recall:$recall\n f1:$f1")
  }
}
