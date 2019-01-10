package server.platform.wutiao

import utils.projectProperties

object PropertiesFactory {
  val propertiesPATH: String = "/platform/wutiao/wutiao_user_predict.properties"
  val propertiesLoad: projectProperties = new projectProperties(propertiesPATH)
  lazy val title = propertiesLoad.getProperties("title")
  lazy val FeatureTable = propertiesLoad.getProperties("FeatureTable")
  lazy val TmpDataPath=propertiesLoad.getProperties("TmpDataPath")
  lazy val eventLogTable=propertiesLoad.getProperties("eventLogTable")
  lazy val trainDatads = propertiesLoad.getProperties("trainDatads")
  lazy val UserportraitCountHiveTable = propertiesLoad.getProperties("UserportraitCountHiveTable")
//  lazy val trainDatads = propertiesLoad.getProperties("trainDatads")
  lazy val PredictModelName=propertiesLoad.getProperties("PredictModelName")
  lazy val PredictModelName_age=propertiesLoad.getProperties("PredictModelName_age")
  lazy val PredictModelName_sex=propertiesLoad.getProperties("PredictModelName_sex")
  lazy val ModelPath=propertiesLoad.getProperties("ModelPath")
  lazy val PredictModelAge_Path=ModelPath+"/"+PredictModelName_age
  lazy val PredictModelSex_Path=ModelPath+"/"+PredictModelName_sex
  lazy val AgeModelOutColName=propertiesLoad.getProperties("AgeModelOutColName")
  lazy val SexModelOutColName=propertiesLoad.getProperties("SexModelOutColName")

  lazy val crossValidModelPath = propertiesLoad.getProperties("crossValidModelPath")
  lazy val crossValidPredictModelAge_Path=crossValidModelPath+"/"+PredictModelName_age
  lazy val crossValidPredictModelSex_Path=crossValidModelPath+"/"+PredictModelName_sex
  lazy val ResultProTable = propertiesLoad.getProperties("ResultProTable")

  lazy val userLogTable = propertiesLoad.getProperties("userLogTable")


}
