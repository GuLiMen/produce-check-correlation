package cn.qtech.bigdata.util

import java.io._
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReader {

  def getConf = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new Configuration
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    conf.set("fs.defaultFS", "hdfs://nameservice")
    conf.set("dfs.nameservices", "nameservice")
    conf.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02")
    conf.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020")
    conf.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020")
    conf.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    conf
  }

  @throws[IOException]
  def write(time: String,path:String): Unit = {

    val cal = Calendar.getInstance
    cal.add(Calendar.MINUTE, 0)
//        val time = cal.getTime
//        val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)

    var fileSystem: FileSystem = null
    fileSystem = FileSystem.get(getConf)
    try {
      val output: OutputStream = fileSystem.create(new Path(path))
//      val output: OutputStream = fileSystem.create(new Path(as))

      // 使用\t作为分隔符
      output.write(time.getBytes("UTF-8"))

      fileSystem.close()
      output.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def readByCsv(path: String, spark: SparkSession): DataFrame = {
    var fileSystem: FileSystem = null
    fileSystem = FileSystem.get(getConf)


    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(path)
    df
  }

  def readTime(readPath: String, spark: SparkSession): String = {

    val lines = spark.read.textFile(readPath).rdd.collect()
    val datetime = lines.head
    datetime
  }

}
