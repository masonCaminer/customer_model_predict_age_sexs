package server.platform.mergeDataXy_Mg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import server.platform.mergeDataXy_Mg.train_predict._
import utils.SparkGlobalSession.buildSparkSession
import utils.TimeUtil
import PropertiesFactory._
import ModelInputOutput._

/**
  * 训练性别
  */
object train_predictModel_merge_sex {
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
    val xy_dbPath: String =
      s"""/user/hive/warehouse/data_mining.db/bdl_xy_age_sex_predict_fea/ds=xy_valid_train_2018-09-26/"""
    val mg_dbPath: String =
      s"""/user/hive/warehouse/data_mining.db/bdl_mg_age_sex_predict_fea/ds=mg_valid_train_2018-09-27/"""
    val trainRawData_xy = getModelInputFromHive_train(xy_dbPath, spark, "sex")
    val trainRawData_mg = getModelInputFromHive_train(mg_dbPath, spark, "sex")
    val trainRawDatas_xy = sampling(trainRawData_xy,"xy")
    println(trainRawDatas_xy.count())
    val trainRawDatas_mg = sampling(trainRawData_mg,"mg")
    println(trainRawDatas_mg.count())
    val trainRawDatas = trainRawDatas_xy.union(trainRawDatas_mg).coalesce(200)
    trainRawDatas.cache()

    /**
      * 1、交叉验证
      */
    //    fit_model_v2(trainRawDatas, "sex") //交叉验证
    //    val model = loadPredictModelCrossValidatorModel(crossValidPredictModelSex_Path)

    /**
      * 2、一般训练
      */
    //    fit_model(trainRawDatas, "sex")
    val model = loadPredictModelRandomForestClassificationModel(PredictModelSex_Path)

    /**
      * 3、手动验证
      */
    vaild_metric(trainRawDatas, model, "sex", true)
    val t2 = (System.currentTimeMillis() - t1) / 1000
    println(s"The time cost $t2 second !")
    spark.stop()
  }

  /**
    * 对2多次采样
    *
    * @param trainRawData
    * @return
    */
  def sampling(trainRawData: sql.DataFrame, data: String): sql.DataFrame = {
    val one = trainRawData.filter("Ydata=1")
    println(one.count())
    val two = trainRawData.filter("Ydata=2")
    println(two.count())
    var rf: sql.DataFrame = one.union(two).union(two)
    if (data == "mg") {
      rf = one.union(two).union(two).union(two).union(two).union(two)
    }
    rf.coalesce(50)
    rf
  }

}
