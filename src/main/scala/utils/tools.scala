package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.fs.{Hdfs, Path}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object spark_tools {

  def parseCreateHiveTableSqlFromSchema(schema: StructType) = {
    /*
     *
     * 功能描述: 把dataframe 的schema 输出（按照sql创建表的形式）
     *
     * @param: [schema]
     * @return: void
     * @auther: hesheng
     * @date: 2018/8/22 16:10
     */
    val tableName = "data_mining.bdl_mygl_big_r_train_predict"
    println(s"CREATE TABLE IF NOT EXISTS $tableName(")
    schema.fields.map(x => (x.name, x.dataType match {
      case v: StringType => "string"
      case v: DoubleType => "double"
      case v: LongType => "bigint"
      case v: FloatType => "float"
      case v: IntegerType => "int"
      case v: DecimalType => "DECIMAL"
    })).foreach(x => println(s"${x._1} ${x._2},"))
    println(")partitioned by(ds string COMMENT 'date');")
  }

  def unionALLfileFromTemp(curDay: String, insertTable: String
                           , path: String
                           , spark: SparkSession) = {
    /*
     *
     * 功能描述: 把临时存储在path目录下的parquet数据，合并写入insertTable Hive表中
     *
     * @param: [curDay, insertTable, path, spark]
     * @return: java.lang.Object
     * @auther: hesheng
     * @date: 2018/8/22 16:11
     */
    val tempPathBuffer = getDirListFile(path, spark)
    if (tempPathBuffer.nonEmpty) {
      val AllTrainData = tempPathBuffer.map(x => {
        readTempParquet(spark, x)
      }).filter(x => !x.rdd.isEmpty()).reduce((u1, u2) => u1.union(u2))
      println(s"insert number of ${AllTrainData} data to Table:$insertTable on train_$curDay")
      AllTrainData.repartition(10).createOrReplaceTempView("AllTrainData")
      spark.sql(
        s"""
           |insert overwrite table $insertTable
           |partition(ds='train_$curDay')
           |select * from AllTrainData
          """.stripMargin)

    }
  }
   /*
    *
    * 功能描述: 从hdfs上删除hive表N日前分区的数据，默认为7日
    *
    * @param:
    * @return:
    * @auther: hesheng
    * @date: 2018/10/8 13:44
    */
  def delete_HiveDsBeforeNum(tableName:String,curds:String,num:Int,spark:SparkSession,
                             assertDataBase:String="data_mining")={
    val set_ds=timetool.lastDs(last = num,timeTemp =timetool.getTimeCalendar(curds) )
    assert(tableName.split("\\.")(0)==assertDataBase)
    val lastNumDsPath=
      s"""/user/hive/warehouse/${tableName.split("\\.")(0)}.db/${tableName.split("\\.")(1)}/ds=$set_ds/""".stripMargin
    val hdfs=org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val hdfs_path=new Path(lastNumDsPath)
    if (hdfs.exists(hdfs_path)){
      hdfs.delete(hdfs_path,true)
      println(s"complete delete hive table $tableName on ds=$set_ds")
    }else{
      println(s"not exist path:$lastNumDsPath")
    }
  }
   /*
    *
    * 功能描述: 删除hive 外部表的N日前分区，需要在删除元数据后操作，否则原始数据依旧存在
    *
    * @param:
    * @return:
    * @auther: hesheng
    * @date: 2018/10/8 13:46
    */
  def delete_ExternalTablefromHive(tableName:String,curds:String,num:Int,spark:SparkSession,assertDataBase:String="data_mining")={
    val set_ds=timetool.lastDs(last = num,timeTemp =timetool.getTimeCalendar(curds) )
    assert(tableName.split("\\.")(0)==assertDataBase)
    spark.sql(
      s"""
        |alter table $tableName drop if exists partition(ds='$set_ds')
      """.stripMargin)
    println(s"delete external hive table $tableName if exists ds=$set_ds ")
  }


/*
  /**
    *
    * 功能描述: 把data数据写入path中，parquet格式
    *
    * @param: [data, path]
    * @return: void
    */
*/
  def saveTempParquet(data: DataFrame, path: String) = {
    println(path)
    data.repartition(10).write.mode("overwrite").parquet(path)
  }
/*
  /**
   *
   * 功能描述: 读取路径的data，parquet格式
   *
   * @param: [spark, path]
   * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   */
*/
  def readTempParquet(spark: SparkSession, path: String) = {
    spark.read.parquet(path)
  }

  def getDirListFile(path: String, spark: SparkSession) = {
    /*
     *
     * 功能描述: 获取目录path下的全部子路径
     *
     * @param: [path, spark]
     * @return: java.lang.String[]
     * @auther: hesheng
     * @date: 2018/8/22 16:13
     */
    val hdfs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val Hpath = new Path(path)
    val pathList = if (hdfs.exists(Hpath)) {
      hdfs.listStatus(Hpath).filter(x => hdfs.listStatus(x.getPath()).nonEmpty).map(x => x.getPath.toString)
    } else {
      Array.empty[String]
    }
    pathList
  }

}

object timetool extends Logging {
  def getds_curDayAndwindowsfirstDay_lastDay(windows: Int, timeTemp: Calendar = Calendar.getInstance(),
                                             format: String = "yyyy-MM-dd") = {
    /*
     *
     * 功能描述: 获取指定日期timeTemp的关于时间窗的一组以format格式输出的时间string数据
     * （curDay:指定日期，firstDay:时间窗最早边界，lastDay:时间窗最晚边界）
     * 如windows=7 curDay=2018-08-08
     * firstDay=2018-08-01 firstDay=2018-08-07
     *
     * @param: [windows, timeTemp, format]
     * @return: scala.Tuple3<java.lang.String,java.lang.String,java.lang.String>
     * @auther: hesheng
     * @date: 2018/8/22 16:18
     */
    assert(windows > 1, "Timewindows must >1")
    val dataSimpleFormate = new SimpleDateFormat(format)
    val curDay = dataSimpleFormate.format(timeTemp.getTime)
    timeTemp.add(Calendar.DATE, -1)
    val lastDay = dataSimpleFormate.format(timeTemp.getTime)
    timeTemp.add(Calendar.DATE, -1 * (windows - 1))
    val firstDay = dataSimpleFormate.format(timeTemp.getTime)
    (curDay, firstDay, lastDay)
  }

  def lastDs(last: Int, format: String = "yyyy-MM-dd", timeTemp: Calendar = Calendar.getInstance()) = {
    /*
     *
     * 功能描述: 获取指定日期timeTemp前last日 时间，并以format格式输出日期string
     *
     * @param: [last, format, timeTemp]
     * @return: java.lang.String
     * @auther: hesheng
     * @date: 2018/8/22 16:14
     */
    val dataSimpleFormate = new SimpleDateFormat(format)
    timeTemp.add(Calendar.DATE, -1 * last)
    dataSimpleFormate.format(timeTemp.getTime)
  }

  def getTimeCalendar(thisDay: String, format: String = "yyyy-MM-dd"): Calendar = {
    /*
     *
     * 功能描述: 获取指定thisDay的format格式下的Calendar对象
     *
     * @param: [thisDay, format]
     * @return: Calendar
     * @auther: hesheng
     * @date: 2018/8/24 11:35
     */
    val timeTemp = Calendar.getInstance()
    timeTemp.setTime(new SimpleDateFormat(format).parse(thisDay))
    timeTemp
  }

  def currentDs(format: String = "yyyy-MM-dd") = {
    lastDs(0)
  }

  def LongTimeToDS(time: Long, format: String = "yyyy-MM-dd") = {
    /*
     *
     * 功能描述: 把linux 13位时间戳转换为format格式的日期string
     *
     * @param: [time, format]
     * @return: java.lang.String
     * @auther: hesheng
     * @date: 2018/8/22 16:17
     */
    new SimpleDateFormat(format).format(new Date(time))
  }

  def LongTimeToDS_str(time: String, format: String = "yyyy-MM-dd") = {
    LongTimeToDS(time.toLong, format)
  }

}

object modelload {
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
}

