package server.game.sscq

import org.apache.log4j.{Level, Logger}
import server.game.sscq.train_predict._
import ModelInputOutput._
import PropertiesFactory._
import org.apache.spark.sql
import utils.SparkGlobalSession.buildSparkSession
import utils.TimeUtil

/**
  * 训练年龄
  */
object train_predictModel_sscq_age {
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
          s"""/user/hive/warehouse/${FeatureTable.split("\\.")(0)}.db/$insert_table_bdl/ds=${trainDatads}/"""
     .stripMargin
//    val dbPath: String =
//    s"""/user/hive/warehouse/data_mining.db/bdl_xy_age_sex_predict_fea/ds=xy_valid_train_2018-09-26/"""

    println("dbPath", dbPath)
    val trainRawData = getModelInputFromHive_train(dbPath, spark, "age")
    trainRawData.cache()

    /**
      * 1、交叉验证
      */
    //        fit_model_v2(trainRawDatas, "age")
    //        val model = loadPredictModelCrossValidatorModel(crossValidPredictModelAge_Path)
    /**
      * 2、一般训练
      */
        fit_model(sampling(trainRawData).cache(), "age")
//    val model = loadPredictModelRandomForestClassificationModel(PredictModelAge_Path)

    /**
      * 3、手动验证
      */
//    vaild_metric(trainRawData, model, "age", true)
    val t2 = (System.currentTimeMillis() - t1) / 1000
    println(s"The time cost $t2 second !")
    spark.stop()
  }
  /**
    * 对2多次采样
    * @param trainRawData
    * @return
    */
  def sampling(trainRawData:sql.DataFrame): sql.DataFrame ={
    val one = trainRawData.filter("Ydata=0")
    val data = trainRawData.union(one).union(one).union(one).union(one).union(one).union(one).union(one).union(one)
    data.coalesce(50)
  }

}
