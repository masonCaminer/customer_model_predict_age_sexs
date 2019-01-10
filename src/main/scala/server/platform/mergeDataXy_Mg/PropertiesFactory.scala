package server.platform.mergeDataXy_Mg

import utils.projectProperties

object PropertiesFactory {
  val propertiesPATH: String = "/platform/mergeDataXy_Mg/mergeData.properties"
  val propertiesLoad: projectProperties = new projectProperties(propertiesPATH)
  lazy val title = propertiesLoad.getProperties("title")
  lazy val trainDatads_mg = propertiesLoad.getProperties("trainDatads_mg")
  lazy val trainDatads_xy = propertiesLoad.getProperties("trainDatads_xy")


  lazy val PredictModelName_age=propertiesLoad.getProperties("PredictModelName_age")
  lazy val AgeModelOutColName=propertiesLoad.getProperties("AgeModelOutColName")

  lazy val PredictModelName_sex=propertiesLoad.getProperties("PredictModelName_sex")
  lazy val SexModelOutColName=propertiesLoad.getProperties("SexModelOutColName")

  lazy val ModelPath=propertiesLoad.getProperties("ModelPath")
  lazy val PredictModelAge_Path=ModelPath+"/"+PredictModelName_age
  lazy val PredictModelSex_Path=ModelPath+"/"+PredictModelName_sex
  lazy val crossValidModelPath = propertiesLoad.getProperties("crossValidModelPath")
  lazy val crossValidPredictModelAge_Path=crossValidModelPath+"/"+PredictModelName_age
  lazy val crossValidPredictModelSex_Path=crossValidModelPath+"/"+PredictModelName_sex


}
