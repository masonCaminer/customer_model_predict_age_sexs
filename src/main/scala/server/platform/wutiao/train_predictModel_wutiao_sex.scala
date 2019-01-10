package server.platform.wutiao

import org.apache.log4j.{Level, Logger}
import server.platform.wutiao.train_predict._
import utils.SparkGlobalSession.buildSparkSession
import utils.TimeUtil
import PropertiesFactory._
import ModelInputOutput._

/**
  * 训练性别
  */
object train_predictModel_wutiao_sex {
  val appName: String = "train_predictMode_sex" + s"_$title"
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
    val trainRawData = getModelInputFromHive_train(dbPath, spark, "sex")
    trainRawData.cache()

    /**
      * 1、交叉验证
      */
    //    fit_model_v2(trainRawData, "sex") //交叉验证
    //        val model = loadPredictModelCrossValidatorModel(PredictModelSex_Path)
    /**
      * 2、一般训练
      */
    fit_model(trainRawData, "sex")
    //    val model = loadPredictModelRandomForestClassificationModel(PredictModelSex_Path)
    /**
      * 3、手动验证
      */
    //    vaild_metric(trainRawData, model, "sex", true)
    val t2 = (System.currentTimeMillis() - t1) / 1000
    println(s"The time cost $t2 second !")
    spark.stop()
  }

}
