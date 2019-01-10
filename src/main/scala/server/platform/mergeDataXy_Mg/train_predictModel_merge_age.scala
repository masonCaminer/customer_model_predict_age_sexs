package server.platform.mergeDataXy_Mg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import server.platform.mergeDataXy_Mg.ModelInputOutput.getModelInputFromHive_train
import server.platform.mergeDataXy_Mg.train_predict._
import PropertiesFactory._
import utils.SparkGlobalSession.buildSparkSession
import utils.TimeUtil

/**
  * 训练年龄
  */
object train_predictModel_merge_age {
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
    val xy_dbPath: String =
      s"""/user/hive/warehouse/data_mining.db/bdl_xy_age_sex_predict_fea/ds=xy_valid_train_2018-09-26/"""
    val mg_dbPath: String =
      s"""/user/hive/warehouse/data_mining.db/bdl_mg_age_sex_predict_fea/ds=mg_valid_train_2018-09-27/"""
    val trainRawData_xy = getModelInputFromHive_train(xy_dbPath, spark, "age")
    val trainRawData_mg = getModelInputFromHive_train(mg_dbPath, spark, "age")
    //    val trainRawDatas_xy = sampling(trainRawData_xy, "xy")
    //    val trainRawDatas_mg = sampling(trainRawData_mg, "mg")
    val trainRawDatas = trainRawData_xy.union(trainRawData_mg)
    trainRawDatas.cache()

    /**
      * 1、交叉验证
      */
    //        fit_model_v2(trainRawDatas, "age")
    //        val model = loadPredictModelCrossValidatorModel(crossValidPredictModelAge_Path)
    /**
      * 2、一般训练
      */
    //    fit_model(trainRawDatas, "age")
    val model = loadPredictModelRandomForestClassificationModel(PredictModelAge_Path)

    /**
      * 3、手动验证
      */
    vaild_metric(trainRawDatas, model, "age", true)
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
    //    val one = trainRawData.filter("Ydata=0")
    val two = trainRawData.filter("Ydata=1")
    //    val three = trainRawData.filter("Ydata=2")
    //    val four = trainRawData.filter("Ydata=3")
    val five = trainRawData.filter("Ydata=4")
    val six = trainRawData.filter("Ydata=5")
    //    val seven = trainRawData.filter("Ydata=6")
    //    val eight = trainRawData.filter("Ydata=7")
    var rf: sql.DataFrame = trainRawData
      .union(two).union(two).union(five).union(five).union(five).union(five).union(six).union(six).union(six).union(six)
    rf.coalesce(50)
  }

}
