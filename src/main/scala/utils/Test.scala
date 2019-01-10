package utils

import java.text.SimpleDateFormat
import java.util.Calendar

import server.game.sscq.FeatureExport.checkIdCard

/**
  * @program: customer_model_predict_age_sex
  * @description: ${description}
  * @author: maoyunlong
  * @create: 2018-11-09 09:47
  **/
object Test {
  def main(args: Array[String]) {

    val isTrueIdCard = checkIdCard("371421797908083336")
    val birthday = "797908089"
    val real_card = "371421797908083336"
    val real_birthday = if (birthday.matches("^[0-9]{8}$") && birthday.slice(0, 4) >= "1938" && birthday.slice(0, 4) <= TimeUtil.getYear()&& birthday.slice(4, 6)
      <= "12" && birthday.slice(6, 8) <= "31") {
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
  }
}
