package server.platform.wutiao

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import server.platform.wutiao.train_predict._
import ModelInputOutput._
import PropertiesFactory._
import utils.SparkGlobalSession.buildSparkSession
import utils.TimeUtil

/**
  * 训练年龄
  */
object train_predictModel_wutiao_age {
  val appName: String = "train_predictMode_age" + s"_$title"
  val num_excutor: Int = 30
  val num_core: Int = 2

  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = buildSparkSession(num_excutor, num_core, appName)
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    val cur_ds = TimeUtil.currentDs()
    val insert_table_bdl = FeatureTable.split("\\.")(1)
    val dbPath: String =
      s"""/user/hive/warehouse/${FeatureTable.split("\\.")(0)}.db/$insert_table_bdl/ds=${trainDatads}/""".stripMargin
    println("dbPath", dbPath)
    val trainRawData = getModelInputFromHive_train(dbPath, spark, "age")
    trainRawData.cache()

    /**
      * 1、交叉验证
      */
    //    fit_model_v2(trainRawData, "age")
    //    val model = loadPredictModelCrossValidatorModel(PredictModelAge_Path)
    /**
      * 2、一般训练
      */
    fit_model(trainRawData, "age")
    //    val model = loadPredictModelRandomForestClassificationModel(PredictModelAge_Path)

    /**
      * 3、手动验证
      */
    //    vaild_metric(trainRawDatas, model, "age", true)
    val t2 = (System.currentTimeMillis() - t1) / 1000
    println(s"The time cost $t2 second !")
    spark.stop()
  }
}
