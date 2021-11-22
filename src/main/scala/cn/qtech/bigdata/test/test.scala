package cn.qtech.bigdata.test

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}

import cn.qtech.bigdata.CBProduceCheckLink

import scala.io.Source


object test {

  def main(args: Array[String]): Unit = {

//    val now: Date = new Date()
//    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val date = dateFormat.format(now)
//
//    val cal = Calendar.getInstance
//    cal.add(Calendar.MINUTE, -20)
//    val time = cal.getTime
//    val yestoday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
//    println(yestoday)
//
////    println(yestoday)
//
//    val guChengTime = "src/main/resources/file/gCTime"
//
//    val lastTime = new StringBuilder
//    val unit = scala.io.Source.fromFile(guChengTime,"UTF-8").getLines().foreach(
//      line => {
//        lastTime.append(line)
//      }
//    )
//    var endTime:String = lastTime.toString()
//
//    print("更新前时间为"+endTime)

//    val writer = new PrintWriter(new File(guChengTime))
//    writer.write(date)
//    writer.close()

    import java.io.FileWriter
//    val writer = new PrintWriter(new File(guChengTime))
//    writer.println()

    val url2: String = classOf[Nothing].getResource("/cBTime.txt").toString
    val url1: String = classOf[Nothing].getClassLoader.getResource("cBTime.txt").toString
    print(url1)

  }
}
