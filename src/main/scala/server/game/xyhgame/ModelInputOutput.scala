package server.game.xyhgame

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import utils.TimeUtil

object ModelInputOutput {
  /**
    * 从hive里加载模型输入的数据
    *
    * @param dbPath
    * @param spark
    * @return
    */
  def getModelInputFromHive_train(dbPath: String, spark: SparkSession, genre: String): sql.DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    val trainRawData = sc.textFile(dbPath).filter { x =>
      val data = x.split("\001")
      var flag = false
      if (genre == "sex" && data(5).toInt != -1) {
        flag =true
      } else if (genre == "age" && data(6).toInt != -1) {
        flag=true
      }
      flag
    }.
      map(x => {
        val data = x.split("\001")
        assert(data.length == 111, s"hive table length must be 111,but now get length of {${data.length}}")
        val ouid = data(0)
        val sex = data(1).toDouble
        val age = data(2).toDouble
        val label_age= age match {
          case value if value>=0 &&value<11=>0
          case value if value>=11 && value<21=>1
          case value if value>=21 && value<31=>2
          case value if value>=31 && value<41=>3
          case value if value>=41 && value<51=>4
          case value if value>=51 && value<61=>5
          case value if value>=61 && value<71=>6
          case value if value>=71 =>7
          case _=> -1
        }
        val birthday = data(3)
        val real_card = data(4)
        var Ydata = 0.0
        if (genre == "sex") {
          Ydata = data(5).toDouble
        } else if (genre == "age") {
          Ydata = data(6).toDouble
        }
        val Xdata = Vectors.dense(data.slice(7, 111).map(x=>if(x=="") 0.0 else x.toDouble))
        (ouid, sex, age,label_age, birthday, real_card, Xdata, Ydata)
      }).toDF("ouid", "sex", "age","label_age", "birthday", "real_card", "Xdata", "Ydata")
    trainRawData
  }
 /*
  *
  * 功能描述: 从hive里加载每日需要预测的数据
  *
  * @param:
  * @return:
  * @auther: hesheng
  * @date: 2018/9/28 10:28
  */
  def getModelInputFromHive_OneDay(dbPath: String, spark: SparkSession, genre: String): sql.DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    val trainRawData = sc.textFile(dbPath).map(x => {
      val data = x.split("\001")
      assert(data.length == 111, s"hive table length must be 111,but now get length of {${data.length}}")
      val ouid = data(0)
      val sex = data(1).toDouble
      val age = data(2).toDouble
      val label_age= age match {
        case value if value>=0 &&value<11=>0
        case value if value>=11 && value<21=>1
        case value if value>=21 && value<31=>2
        case value if value>=31 && value<41=>3
        case value if value>=41 && value<51=>4
        case value if value>=51 && value<61=>5
        case value if value>=61 && value<71=>6
        case value if value>=71 =>7
        case _=> -1
      }
      val birthday = data(3)
      val real_card = data(4)
      var Ydata = 0.0
      if (genre == "sex") {
        Ydata = data(5).toDouble
      } else if (genre == "age") {
        Ydata = data(6).toDouble
      }
      val Xdata = Vectors.dense(data.slice(7, 111).map(x=>if(x=="") 0.0 else x.toDouble))
      (ouid, sex, age,label_age, birthday, real_card, Xdata, Ydata)
    }).toDF("ouid", "sex", "age","label_age", "birthday", "real_card", "Xdata", "Ydata")
    trainRawData
  }

 /*
  *
  * 功能描述: 把训练数据中特征相同的数据过滤出去
  *
  * @param:
  * @return:
  * @auther: hesheng
  * @date: 2018/9/27 15:14
  */
  def filterValidData(dbPath1:String,insertPath:String,spark:SparkSession)={
    import spark.implicits._
    val sc=spark.sparkContext
    val trainRawData1 = sc.textFile(dbPath1).map{rdd=>
      val data=rdd.split("\001")
      val ouidArr=data.slice(0,7).mkString("\001")
      val xdataStr=data.slice(7,111).mkString("\001")
      (ouidArr,xdataStr)
    }.toDF("ouidArr","xdataStr")
    val validData=trainRawData1.groupBy("xdataStr").agg(functions.max($"ouidArr") as "ouidArr"
      ,functions.count($"ouidArr") as "len").select("ouidArr","len","xdataStr").filter("len<=1").select("ouidArr","xdataStr").rdd.map{row=>
      val ouidArr=row.getAs[String]("ouidArr")
      val xdataStr=row.getAs[String]("xdataStr")
      ouidArr+"\001"+xdataStr
    }
//    "/user/hive/warehouse/data_mining.db/bdl_xy_age_sex_predict_fea/ds=xy_valid_train_2018-09-26/"
    val hdfs=org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val hdfs_path=new Path(insertPath)
    if (hdfs.exists(hdfs_path)) {
      hdfs.delete(hdfs_path, true)
    }
    validData.saveAsTextFile(insertPath)
    val testdata=sc.textFile(insertPath)
    println(s"valid data path is :${insertPath} with number ${testdata.count()}")


  }

  /**
    * 保存预测值
    *
    * @param data
    * @param curDs
    * @param dbtable
    * @return
    */
  def OutputModelPredictIntoHiveTable(data:DataFrame,curDs:String,
                                      dbtable:String)={
    val spark=data.sparkSession
    println(s"prepare to insert data into HiveTable $dbtable\n")
    val cur_ds=if(curDs.isEmpty)TimeUtil.currentDs() else curDs
    data.repartition(10).createOrReplaceTempView("result")
    spark.sql(
      s"""
        | insert overwrite table $dbtable partition(ds='$cur_ds')
        | select uid,real_sex,real_age,label_age,model_sex,model_age,game_label
        | from result
      """.stripMargin)
  }


}
