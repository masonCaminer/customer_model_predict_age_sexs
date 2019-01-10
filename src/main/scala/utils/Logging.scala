package utils

import org.apache.log4j.{Level, Logger}

trait Logging {
   /*
    *
    * 功能描述:获取logger
    *
    * @param:
    * @return:
    * @auther: hesheng
    * @date: 2018/8/16 10:18
    */
  // Method to get the logger name for this object
  protected def simpleClassName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }
  lazy val logger =getPrintln
  def getLogger={
    val wutiaoLogger=Logger.getLogger(simpleClassName)
    wutiaoLogger.setLevel(Level.DEBUG)
    wutiaoLogger
  }
  def getPrintln={
    printlnLog.getPrintlnLog
  }
}
class printlnLog{
  def debug(mess:String)={
    println(s"${TimeUtil.LongTimeToDS(System.currentTimeMillis(),"yyyy-MM-dd HH:mm:ss")} DEBUG ${mess}")
  }
  def info(mess:String)={
    println(s"${TimeUtil.LongTimeToDS(System.currentTimeMillis(),"yyyy-MM-dd HH:mm:ss")} INFO ${mess}")
  }
  def error(mess:String)={
    println(s"${TimeUtil.LongTimeToDS(System.currentTimeMillis(),"yyyy-MM-dd HH:mm:ss")} ERROR ${mess}")
  }
  def fatal(mess:String)={
    println(s"${TimeUtil.LongTimeToDS(System.currentTimeMillis(),"yyyy-MM-dd HH:mm:ss")} FATAL ${mess}")
  }
}
object printlnLog{
  def getPrintlnLog=new printlnLog
}


