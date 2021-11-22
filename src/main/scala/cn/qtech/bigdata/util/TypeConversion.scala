package cn.qtech.bigdata.util

object TypeConversion {
  def strTransform(str: Any): String = {
    var value = ""
    if (str != null && !"null".equals(str)&& !"NULL".equals(str) && !"".equals(str)) {
      value = str.toString.trim
    }
    value
  }
  def longTransform(number: Any): Long = {
    var value: Long = 0L
    if (number != null && !"null".equals(number) && !"NULL".equals(number)&& !"".equals(number)) {
      value=number.toString.toLong
    }
    value
  }

  def intTransform(number: Any): Integer = {
    var value:Integer = 0

    if (number != null && !"null".equals(number) && !"NULL".equals(number)&& !"".equals(number)) {
      value = new Integer(number.toString)
    }
    value
  }
}
