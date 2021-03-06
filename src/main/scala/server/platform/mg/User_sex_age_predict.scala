package server.platform.mg

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions
import FeatureExport._
import ModelInputOutput._
import PropertiesFactory._
import train_predict._
import PropertiesFactory.title
import utils.SparkGlobalSession.buildSparkSession
import utils.TimeUtil._
import utils.spark_tools


object User_sex_age_predict {
  val appName: String = "predictMode_age_sexs" + s"_$title"
  val num_excutor: Int = 30
  val num_core: Int = 2

  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = buildSparkSession(num_excutor, num_core, appName)
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    import spark.implicits._

    val cur_ds=if(args.length==0)currentDs() else args(0)
    assert(cur_ds.matches("[\\d]{4}-[\\d]{2}-[\\d]{2}"),s"date must be XXXX-XX-XX but get cur_ds is ${cur_ds}")
    val format=new SimpleDateFormat("yyyy-MM-dd")
    val yester_ds=lastDs(1,timeTemp = setCalendarTime(cur_ds,format))
    println(s"current day is ${cur_ds} and get yesterday User Data insert into ${FeatureTable}")
    saveTempInputData(UserportraitCountHiveTable = UserportraitCountHiveTable,UserTableName = userLogTable,
      InsertFeatureTableName=FeatureTable,ds=yester_ds,mode = "predict",spark=spark)
    val yesterDataPath=
      s"""/user/hive/warehouse/${FeatureTable.split("\\.")(0)}.db/${FeatureTable.split("\\.")(1)}/ds=$yester_ds/""".stripMargin
    val trainRawData=getModelInputFromHive_OneDay(yesterDataPath,spark,"age")
    val outPutTable=ResultProTable
    println("load models..")
    val model_predict_age=loadPredictModelRandomForestClassificationModel(PredictModelAge_Path)
    val model_predict_sex=loadPredictModelRandomForestClassificationModel(PredictModelSex_Path)
    println("predict age and sexs")
    val tmp1_result=model_predict_age.transform(trainRawData).select($"ouid" as "uid",$"sex" as "real_sex",$"age" as "real_age",$"label_age"
      ,$"${AgeModelOutColName}" as "model_age",functions.lit(null) as "game_label",$"Xdata")
    val result=model_predict_sex.transform(tmp1_result)
      .select($"uid",$"real_sex",$"real_age",$"label_age"
        ,$"${SexModelOutColName}" as "model_sex",$"model_age",$"game_label")
    OutputModelPredictIntoHiveTable(result,yester_ds,ResultProTable)

//    删除特征中间表和结果表的前7日当天数据
    spark_tools.delete_HiveDsBeforeNum(FeatureTable,yester_ds,7,spark)
    spark_tools.delete_ExternalTablefromHive(FeatureTable,yester_ds,7,spark)
    spark_tools.delete_HiveDsBeforeNum(ResultProTable,yester_ds,7,spark)
    spark_tools.delete_ExternalTablefromHive(ResultProTable,yester_ds,7,spark)
    val t2 = (System.currentTimeMillis() - t1) / 1000
    println(s"The time cost $t2 second !")
    spark.stop()

  }
}

