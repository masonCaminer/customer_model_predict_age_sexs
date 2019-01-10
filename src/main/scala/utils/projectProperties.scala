package utils

import java.util.Properties

class projectProperties(path:String) extends Logging {
  private val prop=new Properties()
  prop.load(this.getClass.getResourceAsStream(path))
  def getProperties(key:String)={
    prop.getProperty(key) match {
      case null=>{
        throw  new Error(s"$key is not exist in path:$path")
      }
      case x=>{
        logger.info(s"$key set with $x")
        x
      }
    }
  }

}

object projectProperties{
}
