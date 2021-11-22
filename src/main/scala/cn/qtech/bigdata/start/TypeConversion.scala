package cn.qtech.bigdata.start

object TypeConversion {
  def strTransform(str: Any): String = {
    var value = ""
    if (str != null && !"null".equals(str)&& !"NULL".equals(str) && !"".equals(str)) {
      value = str.toString.trim
    }
    value
  }
  def intTransform(number: Any): Integer = {
    var value: Integer = 0

    if (number != null && !"null".equals(number) && !"NULL".equals(number)&& !"".equals(number)) {
      value = new Integer(number.toString)
    }
    value
  }


}
