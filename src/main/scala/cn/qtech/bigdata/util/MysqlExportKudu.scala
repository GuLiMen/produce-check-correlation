package cn.qtech.bigdata.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util

import org.apache.kudu.spark.kudu._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MysqlExportKudu {
  var host, port, username, password, database, table, columns, isAll, condition, writeDatabase, writeTable: String = _
  var columnArray: Array[String] = Array()

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    host = spark.sparkContext.getConf.get("spark.mysql.parameter.host")
    port = spark.sparkContext.getConf.get("spark.mysql.parameter.port")
    username = spark.sparkContext.getConf.get("spark.mysql.parameter.username")
    password = spark.sparkContext.getConf.get("spark.mysql.parameter.password")
    database = spark.sparkContext.getConf.get("spark.mysql.parameter.database")
    table = spark.sparkContext.getConf.get("spark.mysql.parameter.table")
    columns = spark.sparkContext.getConf.get("spark.mysql.parameter.columns")
    columnArray = columns.split(",")
    val columnArrayBroad: Broadcast[Array[String]] = spark.sparkContext.broadcast(columnArray)
    isAll = spark.sparkContext.getConf.get("spark.mysql.parameter.isAll")
    condition = spark.sparkContext.getConf.get("spark.mysql.parameter.condition")
    writeDatabase = spark.sparkContext.getConf.get("spark.write.database")
    writeTable = spark.sparkContext.getConf.get("spark.write.table")

    /**
     * all: 1
     * one: 0
     */
    if ("1".equals(isAll)) condition = "1 = 1"
    mysqlExportKudu(spark, columnArrayBroad)

    spark.stop()
  }


  def readMysqlToRow(spark: SparkSession, columnArrayBroad: Broadcast[Array[String]]): RDD[Row] = {
  val aaa = spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${host}:${port}/${database}?useSSL=false&useUnicode=true&characterEncoding=utf-8",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> s"(select * from ${table} where ${condition} ) as t1",
        "user" -> username,
        "password" -> password
      )).load().rdd
    aaa.take(10).foreach(println(_))
    aaa.map(x => {
      val list = new util.ArrayList[String]()
      for (col <- columnArrayBroad.value) {
        list.add(TypeConversion.strTransform(x.getAs(col)))
      }
      list.add(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
      Row.fromSeq(list.toArray().toSeq)
    })
  }

  def mysqlExportKudu(spark: SparkSession, columnArrayBroad: Broadcast[Array[String]]): Unit = {
    val targetDF = spark.createDataFrame(readMysqlToRow(spark, columnArrayBroad),
      StructType(columnArray.map(x => StructField(x.toLowerCase(), StringType, nullable = true))
        :+ StructField("from_mysql_time", StringType, nullable = true))).cache()
    val tabeleName = writeDatabase + "." + writeTable
    val kuduMaster = "bigdata01:7051,bigdata02:7051,bigdata03:7051"
    /*
        val kuduOptions = Map("kudu.table" -> tabeleName,
          "kudu.master" -> "bigdata01:7051,bigdata02:7051,bigdata03:7051")
    */

    //targetDF.write.options(kuduOptions).mode("append").kudu
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    kuduContext.upsertRows(targetDF, tabeleName, new KuduWriteOptions(false, true))
  }
}