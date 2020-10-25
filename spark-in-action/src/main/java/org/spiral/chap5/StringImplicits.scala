package org.spiral.chap5

import java.sql.Timestamp

/**
  * ${DESCRIPTION}
  *
  * @author dengguoqing
  * @date 2020/1/3
  * @since 1.0 Version
  * @copyright spiral
  */
object StringImplicits {

  implicit class StringImprovements(val s: String) {

    import scala.util.control.Exception.catching

    def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt

    def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong

    def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
  }


}
