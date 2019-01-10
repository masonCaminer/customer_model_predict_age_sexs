package server.platform.wutiao

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import server.platform.wutiao.PropertiesFactory._
import utils.SparkGlobalSession.buildSparkSession
import utils.TimeUtil
import ModelInputOutput._

/*
  * @program: customer_model_predict_age_sex
  * @description: xy 特征
  * @author: maoyunlong
  * @create: 2018-09-20 13:19
  **/
object FeatureExport {
  val appName: String = "sex_age_predict_Extract_" + s"$title"
  val num_excutor: Int = 60
  val num_core: Int = 2

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val t1 = System.currentTimeMillis()
    val spark = buildSparkSession(num_excutor, num_core, appName)
    spark.sparkContext.setLogLevel("ERROR")

    val defaultRepartition_def = num_excutor * num_core * 6
    val lastNum = 30
    val yesterds = TimeUtil.lastDs(1)
    val yester_ds = if (args.length == 0) yesterds else args(0)
    saveTempInputData(UserportraitCountHiveTable, userLogTable, FeatureTable, yester_ds, mode = "train", spark = spark)
    val yesterDataPath =
      s"""/user/hive/warehouse/${FeatureTable.split("\\.")(0)}.db/${FeatureTable.split("\\.")(1)
      }/ds=${title}_train_${yester_ds}/""".stripMargin
    val yesterValidDataPath =
      s"""/user/hive/warehouse/${FeatureTable.split("\\.")(0)}.db/${FeatureTable.split("\\.")(1)
      }/ds=${title}_valid_train_${yester_ds}/""".stripMargin
//    filterValidData(yesterDataPath, yesterValidDataPath, spark)
    val t2 = (System.currentTimeMillis() - t1) / 1000
    println(s"The time cost $t2 second !")
    spark.stop()

  }

  def saveTempInputData(UserportraitCountHiveTable: String, UserTableName: String, InsertFeatureTableName: String,
                        ds: String, mode: String = "predict", spark: SparkSession) = {
    import spark.implicits._
    mode match {
      case "predict" => {
        //        预测的时候 cnxy_user 和 画像宽表join以 画像用户为主，因为cnxy_user为业务方同步数据，业务方会清楚僵尸用户，宽表是从日志的全量用户
        //        用 left join
        val UserportraitOneDayFeature = ExtractFeature(UserportraitCountHiveTable, ds, spark)
        val CorrectYlable = getUser_id_sex_birthday_age(UserTableName, ds, spark)
        val FeatureOneDay = UserportraitOneDayFeature.join(CorrectYlable, $"uid" === $"id", "left")
          .select(
            $"uid" as "ouid",
            $"sex",
            $"age",
            $"birthday",
            $"real_card",
            $"sex_Y",
            $"age_Y",
            $"latest_pc_login_day",
            $"first_pc_login_day",
            $"pc_login_times",
            $"pc_login_avgtimes_m",
            $"pc_login_mediantimes_m",
            $"login_time_segment_avg",
            $"login_time_segment_median",
            $"pc_login_week_num_Mon",
            $"pc_login_week_num_Tue",
            $"pc_login_week_num_Wed",
            $"pc_login_week_num_Thu",
            $"pc_login_week_num_Fri",
            $"pc_login_week_num_Sat",
            $"pc_login_week_num_Sun",
            $"pc_login_hour_num_0",
            $"pc_login_hour_num_1",
            $"pc_login_hour_num_2",
            $"pc_login_hour_num_3",
            $"pc_login_hour_num_4",
            $"pc_login_hour_num_5",
            $"pc_login_hour_num_6",
            $"pc_login_hour_num_7",
            $"pc_login_hour_num_8",
            $"pc_login_hour_num_9",
            $"pc_login_hour_num_10",
            $"pc_login_hour_num_11",
            $"pc_login_hour_num_12",
            $"pc_login_hour_num_13",
            $"pc_login_hour_num_14",
            $"pc_login_hour_num_15",
            $"pc_login_hour_num_16",
            $"pc_login_hour_num_17",
            $"pc_login_hour_num_18",
            $"pc_login_hour_num_19",
            $"pc_login_hour_num_20",
            $"pc_login_hour_num_21",
            $"pc_login_hour_num_22",
            $"pc_login_hour_num_23",
            $"pc_login_time_segment_min",
            $"pc_login_time_segment_max",
            $"pc_login_time_segment_avg",
            $"pc_login_time_segment_std",
            $"pc_city_array_set_count",
            $"pc_prov_array_set_count",
            $"pc_coun_array_set_count",
            $"pc_ip_array_set_count",
            $"latest_pc_visit_in_time",
            $"first_pc_visit_in_time",
            $"latest_pc_os",
            $"first_pc_os"
          ).na.fill(Map("sex" -> -1, "age" -> -1, "sex_Y" -> -1, "age_Y" -> -1))
        FeatureOneDay.repartition(10).createOrReplaceTempView("FeatureOneDay")
        spark.sql(
          s"""
             |insert overwrite table $InsertFeatureTableName
             |partition(ds='${ds}')
             |select * from FeatureOneDay
          """.stripMargin)
      }
      case "train" => {
        val UserportraitOneDayFeature = ExtractFeature(UserportraitCountHiveTable, ds, spark)
        val CorrectYlable = getUser_id_sex_birthday_age(UserTableName, ds, spark)
        val FeatureOneDay = UserportraitOneDayFeature.join( CorrectYlable, $"uid" === $"id", "inner").select(
          $"uid" as "ouid",
          $"sex",
          $"age",
          $"birthday",
          $"real_card",
          $"sex_Y",
          $"age_Y",
          $"latest_pc_login_day",
          $"first_pc_login_day",
          $"pc_login_times",
          $"pc_login_avgtimes_m",
          $"pc_login_mediantimes_m",
          $"login_time_segment_avg",
          $"login_time_segment_median",
          $"pc_login_week_num_Mon",
          $"pc_login_week_num_Tue",
          $"pc_login_week_num_Wed",
          $"pc_login_week_num_Thu",
          $"pc_login_week_num_Fri",
          $"pc_login_week_num_Sat",
          $"pc_login_week_num_Sun",
          $"pc_login_hour_num_0",
          $"pc_login_hour_num_1",
          $"pc_login_hour_num_2",
          $"pc_login_hour_num_3",
          $"pc_login_hour_num_4",
          $"pc_login_hour_num_5",
          $"pc_login_hour_num_6",
          $"pc_login_hour_num_7",
          $"pc_login_hour_num_8",
          $"pc_login_hour_num_9",
          $"pc_login_hour_num_10",
          $"pc_login_hour_num_11",
          $"pc_login_hour_num_12",
          $"pc_login_hour_num_13",
          $"pc_login_hour_num_14",
          $"pc_login_hour_num_15",
          $"pc_login_hour_num_16",
          $"pc_login_hour_num_17",
          $"pc_login_hour_num_18",
          $"pc_login_hour_num_19",
          $"pc_login_hour_num_20",
          $"pc_login_hour_num_21",
          $"pc_login_hour_num_22",
          $"pc_login_hour_num_23",
          $"pc_login_time_segment_min",
          $"pc_login_time_segment_max",
          $"pc_login_time_segment_avg",
          $"pc_login_time_segment_std",
          $"pc_city_array_set_count",
          $"pc_prov_array_set_count",
          $"pc_coun_array_set_count",
          $"pc_ip_array_set_count",
          $"latest_pc_visit_in_time",
          $"first_pc_visit_in_time",
          $"latest_pc_os",
          $"first_pc_os"
        )
        FeatureOneDay.filter("sex_Y!=-1 or age_Y!=-1").repartition(10).createOrReplaceTempView("FeatureOneDayWithSex_Age")
        spark.sql(
          s"""
             |insert overwrite table $InsertFeatureTableName
             |partition(ds='${title}_train_${ds}')
             |select * from FeatureOneDayWithSex_Age
          """.stripMargin)

      }
    }

  }

  /**
    * 提取单日画像特征
    *
    * @param cur_ds
    * @param spark
    */
  def ExtractFeature(UserportraitCountHiveTable: String, cur_ds: String, spark: SparkSession)
  = {
    //均方差
    spark.udf.register("variance_of_mean", (arr: Seq[Long]) => {
      val arr_siez = arr.length
      val arr_avg = arr.sum.toDouble / arr.length
      val avg = arr.map { x =>
        Math.pow((x.toDouble - arr_avg), 2)
      }.reduce(_ + _)
      Math.sqrt(avg / arr_siez)
    })
    //最大值
    spark.udf.register("max_value", (arr: Seq[Long]) => {
      arr.max
    })
    //最小值
    spark.udf.register("min_value", (arr: Seq[Long]) => {
      arr.min
    })
    //平均值
    spark.udf.register("avg_value", (arr: Seq[Long]) => {
      arr.sum.toDouble / arr.length
    })
    //    spark.sql(
    //      """
    //        |SELECT uid,first_pc_visit_in_time
    //        |,from_unixtime(cast(first_pc_visit_in_time as int),'HH')as hr FROM
    //        |dana_user_profile.widetab_1023054
    //        |WHERE ds='2018-09-20' and
    //        |first_pc_visit_in_time is not null
    //      """.stripMargin).filter("hr is null ").show()
    val fea = spark.sql(
      s"""
         |SELECT uid,
         |if(latest_pc_login_day is null,3660,latest_pc_login_day) as latest_pc_login_day,
         |if(first_pc_login_day is null,3660,first_pc_login_day) as first_pc_login_day,
         |pc_login_times,
         |if(pc_login_avgtimes_m is null,0,pc_login_avgtimes_m) as pc_login_avgtimes_m,
         |if(pc_login_mediantimes_m is null,0,pc_login_mediantimes_m) as pc_login_mediantimes_m,
         |if(login_time_segment_avg is null,1000000,login_time_segment_avg) as login_time_segment_avg,
         |if(login_time_segment_median is null,1000000,login_time_segment_median) as login_time_segment_median,
         |if (pc_login_week_num['Mon'] is null ,0,pc_login_week_num['Mon'] ) as pc_login_week_num_Mon,
         |if (pc_login_week_num['Tue'] is null ,0,pc_login_week_num['Tue'] ) as pc_login_week_num_Tue,
         |if (pc_login_week_num['Wed'] is null ,0,pc_login_week_num['Wed'] ) as pc_login_week_num_Wed,
         |if (pc_login_week_num['Thu'] is null ,0,pc_login_week_num['Thu'] ) as pc_login_week_num_Thu,
         |if (pc_login_week_num['Fri'] is null ,0,pc_login_week_num['Fri'] ) as pc_login_week_num_Fri,
         |if (pc_login_week_num['Sat'] is null ,0,pc_login_week_num['Sat'] ) as pc_login_week_num_Sat,
         |if (pc_login_week_num['Sun'] is null ,0,pc_login_week_num['Sun'] ) as pc_login_week_num_Sun,
         |if(pc_login_hour_num['24'] is null,0,pc_login_hour_num['24']) as pc_login_hour_num_0,
         |if(pc_login_hour_num['1'] is null,0,pc_login_hour_num['1']) as pc_login_hour_num_1,
         |if(pc_login_hour_num['2'] is null,0,pc_login_hour_num['2']) as pc_login_hour_num_2,
         |if(pc_login_hour_num['3'] is null,0,pc_login_hour_num['3']) as pc_login_hour_num_3,
         |if(pc_login_hour_num['4'] is null,0,pc_login_hour_num['4']) as pc_login_hour_num_4,
         |if(pc_login_hour_num['5'] is null,0,pc_login_hour_num['5']) as pc_login_hour_num_5,
         |if(pc_login_hour_num['6'] is null,0,pc_login_hour_num['6']) as pc_login_hour_num_6,
         |if(pc_login_hour_num['7'] is null,0,pc_login_hour_num['7']) as pc_login_hour_num_7,
         |if(pc_login_hour_num['8'] is null,0,pc_login_hour_num['8']) as pc_login_hour_num_8,
         |if(pc_login_hour_num['9'] is null,0,pc_login_hour_num['9']) as pc_login_hour_num_9,
         |if(pc_login_hour_num['10'] is null,0,pc_login_hour_num['10']) as pc_login_hour_num_10,
         |if(pc_login_hour_num['11'] is null,0,pc_login_hour_num['11']) as pc_login_hour_num_11,
         |if(pc_login_hour_num['12'] is null,0,pc_login_hour_num['12']) as pc_login_hour_num_12,
         |if(pc_login_hour_num['13'] is null,0,pc_login_hour_num['13']) as pc_login_hour_num_13,
         |if(pc_login_hour_num['14'] is null,0,pc_login_hour_num['14']) as pc_login_hour_num_14,
         |if(pc_login_hour_num['15'] is null,0,pc_login_hour_num['15']) as pc_login_hour_num_15,
         |if(pc_login_hour_num['16'] is null,0,pc_login_hour_num['16']) as pc_login_hour_num_16,
         |if(pc_login_hour_num['17'] is null,0,pc_login_hour_num['17']) as pc_login_hour_num_17,
         |if(pc_login_hour_num['18'] is null,0,pc_login_hour_num['18']) as pc_login_hour_num_18,
         |if(pc_login_hour_num['19'] is null,0,pc_login_hour_num['19']) as pc_login_hour_num_19,
         |if(pc_login_hour_num['20'] is null,0,pc_login_hour_num['20']) as pc_login_hour_num_20,
         |if(pc_login_hour_num['21'] is null,0,pc_login_hour_num['21']) as pc_login_hour_num_21,
         |if(pc_login_hour_num['22'] is null,0,pc_login_hour_num['22']) as pc_login_hour_num_22,
         |if(pc_login_hour_num['23'] is null,0,pc_login_hour_num['23']) as pc_login_hour_num_23,
         |if(pc_login_time_segment is null,0,min_value(pc_login_time_segment)) as pc_login_time_segment_min,
         |if(pc_login_time_segment is null,0,max_value(pc_login_time_segment)) as pc_login_time_segment_max,
         |if(pc_login_time_segment is null,0,avg_value(pc_login_time_segment)) as pc_login_time_segment_avg,
         |if(pc_login_time_segment is null,0,variance_of_mean(pc_login_time_segment)) as pc_login_time_segment_std,
         |size(pc_city_array) as pc_city_array_set_count,
         |size(pc_prov_array) as pc_prov_array_set_count,
         |size(pc_coun_array) as pc_coun_array_set_count,
         |size(pc_ip_array) as pc_ip_array_set_count,
         |if(latest_pc_visit_in_time  is null,0, cast(from_unixtime(cast(latest_pc_visit_in_time as int),'HH') as int) )
         | as  latest_pc_visit_in_time,
         |if(first_pc_visit_in_time is null,0, cast(from_unixtime(cast(first_pc_visit_in_time as int),'HH') as int) )
         |as  first_pc_visit_in_time,
         |if( latest_pc_os='ios',1,0) as latest_pc_os,
         |if( first_pc_os='ios',1,0) as first_pc_os
         |from
         |$UserportraitCountHiveTable
         |where ds ='$cur_ds'
      """.stripMargin)
    fea
  }

  /*
   *
   * 功能描述:得到用户的性别年龄数据 数据格式 "id","sex","age","birthday","real_card","sex_Y","age_Y"
   *
   * @param: sourceTableName:源数据表 ds 日期（一般用户表今天只存在昨日的画像）
   * @return: dataframe
   * @auther: hesheng
   * @date: 2018/9/20 17:50
   */
  def getUser_id_sex_birthday_age(UserTableName: String, ds: String, spark: SparkSession) = {
    import spark.implicits._
    spark.sql(
      s"""
         |SELECT id ,sex ,birthday,real_card from (
         |SELECT cast(uid as int) as id,cast(sex as int) sex, CAST(null as int) as birthday,id_card_num as real_card,
         |Row_number() over (PARTITION BY uid order by ds desc) num
         |FROM ${UserTableName})
         |WHERE num=1
      """.stripMargin).map { row =>
      val id = row.getAs[Int]("id")
      val sex = if (row.isNullAt(1)) 0 else row.getAs[Int]("sex")
      val birthday = if (row.isNullAt(2)) "" else {
        val tmp = row.getAs[Int]("birthday")
        if (tmp != 0) TimeUtil.LongTimeToDS(tmp.toLong * 1000, format = "yyyyMMdd") else ""
      }
      val real_card = if (row.isNullAt(3)) "" else row.getAs[String]("real_card")
      val isTrueIdCard = checkIdCard(real_card)
      val real_sex = if (sex == 1 || sex == 2) {
        sex
      } else if (isTrueIdCard) {
        val sexflag = if (real_card.length == 15) real_card.split("")(14).toInt else real_card.split("")(16).toInt
        if (sexflag % 2 == 0) 2 else 1
      } else {
        -1
      }
      val sex_Y = real_sex
      val real_birthday = if (birthday.matches("^[0-9]{8}$") && birthday.slice(0, 4) >= "1938" && birthday.slice(4,
        6) <= "12" && birthday.slice(6, 8) <= "31") {
        birthday
      } else if (isTrueIdCard) {
        if (real_card.length == 15) "19" + real_card.slice(6, 12) else real_card.slice(6, 14)
      } else {
        "-1"
      }
      val age = if (real_birthday == "-1") -1 else {
        val birthdayCal = Calendar.getInstance()
        birthdayCal.setTime(new SimpleDateFormat("yyyyMMdd").parse(real_birthday))
        val curCal = Calendar.getInstance()
        curCal.get(Calendar.YEAR) - birthdayCal.get(Calendar.YEAR)
      }
      val age_Y = age match {
        case value if value >= 0 && value < 11 => 0
        case value if value >= 11 && value < 21 => 1
        case value if value >= 21 && value < 31 => 2
        case value if value >= 31 && value < 41 => 3
        case value if value >= 41 && value < 51 => 4
        case value if value >= 51 && value < 61 => 5
        case value if value >= 61 && value < 71 => 6
        case value if value >= 71 => 7
        case _ => -1
      }
      (id, real_sex, age, real_birthday, real_card, sex_Y, age_Y)
    }.toDF("id", "sex", "age", "birthday", "real_card", "sex_Y", "age_Y")

    //    spark.sql(
    //      s"""
    //         |SELECT id,cast( if(sex not in (1,2),id_sex,sex) as int) as sex, if(length(birthday)=8,birthday,
    // id_birthday) as birthday,real_card
    //         |FROM(SELECT id,sex, birthday,real_card,length(real_card) as len,real_card,
    //         |if(pmod(cast( if(length(real_card)=15,substring(real_card,15,1),substring(real_card,17,1) ) as
    // bigint),2)=0,1,2 ) as id_sex,
    //         |if(length(real_card)=15,concat('19',substring(real_card,7,6) ),substring(real_card,7,8) ) as id_birthday
    //         |FROM ${UserTableName}
    //         |WHERE ds='${ds}' AND
    //         | sex in(1,2)  and length(birthday)=8 and birthday>='19380000'  or length(real_card) in (15,18) )
    //          """.stripMargin)
    //      .filter{row=>
    //      checkIdCard(row.getAs("real_card"))
    //    }.map{case Row(id:Int,sex:Int,birthday:String,real_card:String)=>
    //      val birthdayCal=Calendar.getInstance()
    //      birthdayCal.setTime(new SimpleDateFormat("yyyyMMdd").parse(birthday))
    //      val curCal=Calendar.getInstance()
    //      val age=curCal.get(Calendar.YEAR)-birthdayCal.get(Calendar.YEAR)
    //      val sex_Y=sex
    //      val age_Y= age match {
    //        case value if value<11=>0
    //        case value if value>=11 && value<21=>1
    //        case value if value>=21 && value<31=>2
    //        case value if value>=31 && value<41=>3
    //        case value if value>=41 && value<51=>4
    //        case value if value>=51 && value<61=>5
    //        case value if value>=61 && value<71=>6
    //        case value if value>=71 =>7
    //        case _=>throw new Error(s"$age")
    //      }
    //      (id.toString,sex,age,birthday,real_card,sex_Y,age_Y)
    //    }.toDF("id","sex","age","birthday","real_card","sex_Y","age_Y").filter("age>=60")
    //       sex_Y 性别 1男 2女  age_Y 年龄段 0-7
  }

  /*
   *
   * 功能描述: 判断身份证号是否合规
   *
   * @param: idCard 身份证号
   * @return:
   * @auther: hesheng
   * @date: 2018/9/20 17:49
   */
  def checkIdCard(idCard: String) = {
    //      位数是否正确
    var isOK = idCard.matches("^[0-9]{15}$") || idCard.matches("^[0-9]{17}[0-9xX]$")
    if (isOK) {
      //        地区码是否正确
      isOK = areaId.contains(idCard.slice(0, 2))
      if (isOK) {
        //          生日是否正确
        val birthday = if (idCard.length == 15) "19" + idCard.slice(6, 12) else idCard.slice(6, 14)
        val year = birthday.slice(0, 4)
        val month = birthday.slice(4, 6)
        val day = birthday.slice(6, 8)
        isOK = year >= "1938" && month <= "12" && day <= "31"
        if (isOK && idCard.length == 18) {
          //            验证校验码
          val weight = Array[Int](7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2).zip(idCard.split("").slice(0,
            17).map(_.toInt))
          val rcr = weight.map(x => x._1 * x._2).sum % 11
          val rcrCode = Array[String]("1", "0", "x", "9", "8", "7", "6", "5", "4", "3", "2")
          isOK = idCard.split("")(17) == rcrCode(rcr)
        }
      }
    }
    isOK
  }

  //身份证 合法地域及数字标识
  private val area = Map(
    "11" -> "北京",
    "12" -> "天津",
    "13" -> "河北",
    "14" -> "山西",
    "15" -> "内蒙古",
    "21" -> "辽宁",
    "22" -> "吉林",
    "23" -> "黑龙江",
    "31" -> "上海",
    "32" -> "江苏",
    "33" -> "浙江",
    "34" -> "安徽",
    "35" -> "福建",
    "36" -> "江西",
    "37" -> "山东",
    "41" -> "河南",
    "42" -> "湖北",
    "43" -> "湖南",
    "44" -> "广东",
    "45" -> "广西",
    "46" -> "海南",
    "50" -> "重庆",
    "51" -> "四川",
    "52" -> "贵州",
    "53" -> "云南",
    "54" -> "西藏",
    "61" -> "陕西",
    "62" -> "甘肃",
    "63" -> "青海",
    "64" -> "宁夏",
    "65" -> "新疆",
    "71" -> "台湾",
    "81" -> "香港",
    "82" -> "澳门",
    "91" -> "国外"
  )
  private val areaId = area.keySet

}
