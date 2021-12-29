package cn.qtech.bigdata.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession

object MysqlExportPhoenix extends Serializable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val host = spark.sparkContext.getConf.get("spark.mysql.host")
    val port = spark.sparkContext.getConf.get("spark.mysql.port")
    val username = spark.sparkContext.getConf.get("spark.mysql.username")
    val password = spark.sparkContext.getConf.get("spark.mysql.password")
    val database = spark.sparkContext.getConf.get("spark.mysql.database")
    val table = spark.sparkContext.getConf.get("spark.mysql.table")
    val columns = spark.sparkContext.getConf.get("spark.mysql.columns")
    val phoenix_url = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.write.phoenix.url"))
    val writeDatabase = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.write.database"))
    val writeTable = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.write.table"))
    val columnArray = spark.sparkContext.broadcast(columns.split(","))

    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://${host}:${port}/${database}?useSSL=false&useUnicode=true&characterEncoding=utf-8",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> s"(select * from ${table}  ) as t1",
        "user" -> username,
        "password" -> password
      )).load()
      .rdd
      .foreachPartition(item => {
        println(phoenix_url.value + "===========phoenix_url.value=============")
        println(writeTable.value + "===========writeTable.value=============")

        val conn = ConnectPhoenixDB.getConnection(phoenix_url.value)
        //  val conn = ConnectPhoenixDB.getConnection("phoenix_url")

        while (item.hasNext) {
          val x = item.next()

          val valuesBuf = new StringBuilder()
          val keysBuf = new StringBuilder()
          for (col <- columnArray.value) {
            keysBuf.append(col).append(",")
            valuesBuf.append("""'""").append(TypeConversion.strTransform(x.getAs(col))).append("""'""").append(",")
          }
          keysBuf.append("from_mysql_time")
          valuesBuf.append("""'""").append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("""'""")
          val upsertSql = s"upsert into ${writeDatabase.value}.${writeTable.value} ($keysBuf) values ($valuesBuf)"


          //插入数据
          PhoenixJDBC.upsertData(conn, upsertSql)


        }
        ConnectPhoenixDB.closeConn(conn)

      })
    spark.stop()
  }


}
