package cn.qtech.bigdata.test
import cn.qtech.bigdata.util.SparkReader.write

object testTime {
  def main(args: Array[String]): Unit = {

    write("2021-10-01 00:00:00","/jobs/spark/AAjointest/gCTime.txt")

  }

}