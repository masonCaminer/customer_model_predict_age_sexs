package utils

import org.apache.spark.sql.SparkSession


object SparkGlobalSession {
  lazy private val default_sparkSession = SparkSession.builder().master("local[*]").appName("TestTest")
    .config("spark.streaming.kafka.maxRatePerPartition", "10").
    config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    config("hive.exec.dynamic.partition.mode", "nonstrict").
    config("hive.exec.max.dynamic.partitions.pernode", 100000000).
    config("spark.Kryoserializer.buffer.max", "128").
    enableHiveSupport().
    config("spark.default.parallelism", 5).
    config("spark.storage.memoryFraction", "0.5").
    config("spark.shuffle.memoryFraction", "0.7").
    config("spark.sql.shuffle.partitions", 5).
    config("spark.yarn.am.memory", "2G").
    config("spark.yarn.executor.memoryOverhead", 1024).
    config("spark.yarn.driver.memoryOverhead", 6144).
    config("spark.driver.maxResultSize", "8g").getOrCreate()
  //sparkSession.sparkContext.setLogLevel("ERROR")

   def getSparkSession(): SparkSession = {
     getSparkSession("default")
  }
  def buildSparkSession(num_excutor:Int,num_core:Int,appName:String):SparkSession={
    SparkSession.builder().master("yarn-client").appName(appName).
      config("hive.exec.dynamic.partition.mode", "nonstrict").
      config("hive.exec.max.dynamic.partitions.pernode", 100000000).
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      enableHiveSupport().
      config("spark.memory.fraction", "0.75").
      config("spark.storage.memoryFraction", "0.5").
      config("spark.shuffle.memoryFraction", "0.7").
      config("spark.sql.shuffle.partitions", num_excutor*num_core*6).
      config("spark.default.parallelism", num_excutor*num_core*3).
      config("spark.ui.showConsoleProgress","false").
      getOrCreate()
  }
  def getSparkSession(model:String):SparkSession={
    model match {
      case "default"=> default_sparkSession
    }
  }


}