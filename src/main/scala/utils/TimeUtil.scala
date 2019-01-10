package utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * 时间处理
  */
object TimeUtil {
  /**
    * yyyy-MM-dd HH:mm:ss
    */
  lazy val defaultDatetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  /**
    * yyyy-MM-dd
    */
  lazy val onlyDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  lazy val tsIncrBy28days = 3600 * 24 * 28 // 以28为单位递增

  /**
    * 当前时间的前几天或后几天
    *
    * @param dateFormat
    * @param num
    * @return
    */
  def nowDate(dateFormat: SimpleDateFormat, num: Int): String = {
    var now: Date = new Date()
    val date = dateFormat.format(now)
    now = dateFormat.parse(date)
    var cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, num)
    val newDate = cal.getTime
    dateFormat.format(newDate)
  }

  /**
    * 获取年份
    * @param dateFormat
    * @param num
    * @return
    */
  def getYear(): String = {

    var cal = Calendar.getInstance()
    val year = String.valueOf(cal.get(Calendar.YEAR))
    year
  }
  /**
    * 转换为日期格式
    *
    * @param ts
    * @return 2018-06-03 14:39:45
    */
  def ts2Date(ts: Long): String = {
    ts2Date(ts, defaultDatetimeFormat)
  }

  /**
    * 时间转换
    *
    * @param ts
    * @param dateFormat
    * @return
    */
  def ts2Date(ts: Long, dateFormat: SimpleDateFormat): String = {
    val tsObj = new Timestamp(ts * 1000)
    dateFormat.format(tsObj)
  }

  /**
    * 转换为今天0点时间戳
    *
    * @param dateStr
    * @return 1532880000
    */
  def date2Ts(dateStr: String): Long = {
    onlyDateFormat.parse(dateStr).getTime / 1000
  }

  def strDateDiff(startDateStr: String, endDateStr: String): Int = {
    ((date2Ts(endDateStr) - date2Ts(startDateStr)) / (3600 * 24)).toInt + 1
  }

  /**
    * 计算距离最终时间的天数+1
    *
    * @param ts
    * @param endDayMorningTs
    * @return
    */
  def daysFromEndDayMorning(ts: Long, endDayMorningTs: Long): Int = {
    ((endDayMorningTs - ts) / (3600 * 24)).toInt + 1
  }

  /**
    * 得到月份
    *
    * @param d
    * @return 2018-07
    */
  def getMonthKey(d: String): String = {
    d.take(7)
  }

  /**
    * 获得时间key
    *
    * @param d
    * @return 14
    */
  def getHourKey(d: String): String = {
    d.substring(11, 13)
  }

  /**
    * 从ts开始的那个月到today的所有monthKey, 包括ts所在的月以及当月，这样会导致月度指标不准确，不过能反映用户首次过来当月以及当前这个月的实时情况
    *
    * @param ts
    * @param endDayMorningTs
    * @return Map(2018-07 -> 0, 2018-06 -> 0)
    */
  def getDefaultMonthCounters(ts: Long, endDayMorningTs: Long): scala.collection.mutable.Map[String, Int] = {
    val defaultMonthCounters = scala.collection.mutable.Map[String, Int]()
    var tsIndex = ts
    while (tsIndex < endDayMorningTs) {
      defaultMonthCounters.put(getMonthKey(ts2Date(tsIndex)), 0)
      tsIndex += tsIncrBy28days
    }
    defaultMonthCounters.put(getMonthKey(ts2Date(endDayMorningTs)), 0)
    defaultMonthCounters
  }

  /**
    * 获得时间map
    *
    * @return
    */
  def getDefaultHourCounter = scala.collection.mutable.Map("00" -> 0, "01" -> 0, "02" -> 0, "03" -> 0, "04" -> 0,
    "05" -> 0, "06" -> 0, "07" -> 0, "08" -> 0, "09" -> 0, "10" -> 0, "11" -> 0, "12" -> 0, "13" -> 0, "14" -> 0,
    "15" -> 0, "16" -> 0, "17" -> 0, "18" -> 0, "19" -> 0, "20" -> 0, "21" -> 0, "22" -> 0, "23" -> 0)

  lazy private val hourCounterKeySorted = getDefaultHourCounter.keys.toList.sorted

  /**
    * 排序并转换为数组
    *
    * @param hourCounter
    * @return
    */
  def getSortedHourCounter(hourCounter: scala.collection.mutable.Map[String, Int]): Array[Int] = {
    hourCounterKeySorted.map(hourKey => hourCounter.getOrElse(hourKey, 0)).toArray
  }

  /**
    * 获取当前日期的前几天的或后几天的日期
    *
    * @param InsertDate
    * @param num
    * @param formate
    * @return
    */
  def getDate(InsertDate: String, num: Int, formate: SimpleDateFormat): String = {
    val timeInstance = TimeUtil.setCalendarTime(InsertDate, formate)
    timeInstance.add(Calendar.DATE, num)
    val thisDs = formate.format(timeInstance.getTime)
    thisDs
  }

  def main(args: Array[String]) {
    val a = getFirstLastDayByThisDayWithWindows("2018-11-07", 90)
    println(a)
  }

  /**
    *
    * 功能描述: 获取今天时间
    *
    * @param: [format]:格式
    * @return: java.lang.String
    * @auther: hesheng
    * @date: 2018/8/16 10:14
    */
  def currentDs(format: String = "yyyy-MM-dd") = {
    lastDs(0)
  }

  /**
    *
    * 功能描述: 获取指定格式的日期字符串
    *
    * @param: [last：获取之前（-N）或者之后（N）日期, format, timeTemp:默认基础日期为今日，可以设定为其他时间]
    * @return: java.lang.String
    */
  def lastDs(last: Int, format: String = "yyyy-MM-dd", timeTemp: Calendar = Calendar.getInstance()) = {
    val dataSimpleFormate = new SimpleDateFormat(format)
    timeTemp.add(Calendar.DATE, -1 * last)
    dataSimpleFormate.format(timeTemp.getTime)
  }

  def LongTimeToDS(time: Long, format: String = "yyyy-MM-dd") = {
    /*
     *
     * 功能描述: 由long格式时间戳获取指定格式字符串
     *
     * @param: [time:时间, format]
     * @return: java.lang.String
     * @auther: hesheng
     * @date: 2018/8/16 10:16
     */
    new SimpleDateFormat(format).format(new Date(time))
  }

  def LongTimeToDS_str(time: String, format: String = "yyyy-MM-dd") = {
    LongTimeToDS(time.toLong, format)
  }

  def setCalendarTime(thisDate: String, formate: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")): Calendar = {
    /*
     *
     * 功能描述: 获取字符串指定格式的时间Calendar实例
     *
     * @param: [thisDate, formate]
     * @return: java.util.Calendar
     * @auther: hesheng
     * @date: 2018/8/16 15:41
     */
    val timeInstance = Calendar.getInstance()
    timeInstance.setTime(formate.parse(thisDate))
    timeInstance
  }

  /**
    *
    * 功能描述: 获取thisday 之前的windows日期区间的上下界，例如thisDay：2018-08-16，firstDay：2018-08-09，lastDay：2018-08-15
    *
    * @param: [thisDay, windows]
    * @return: scala.Tuple2<java.lang.String,java.lang.String>
    * @auther: hesheng
    * @date: 2018/8/16 16:18
    */
  def getFirstLastDayByThisDayWithWindows(thisDay: String, windows: Int) = {
    assert(thisDay.matches("[\\d]{4}-[\\d]{2}-[\\d]{2}"), s"date must be XXXX-XX-XX but get cur_ds is ${thisDay}")
    assert(windows > 1, "windows must be >1")
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val timeCal = setCalendarTime(thisDay, format)
    timeCal.add(Calendar.DATE, -1)
    val lastDay = format.format(timeCal.getTime)
    timeCal.add(Calendar.DATE, -1 * (windows - 1))
    val firstDay = format.format(timeCal.getTime)
    (firstDay, lastDay)
  }

}
