package cn.qtech.bigdata.util

import org.apache.spark.sql.SparkSession


object PhoenixExportKudu {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val database = spark.sparkContext.getConf.get("spark.read.database")
    val table = spark.sparkContext.getConf.get("spark.read.table")
    val columns = spark.sparkContext.getConf.get("spark.read.columns").toUpperCase
    val condition = spark.sparkContext.getConf.get("spark.read.condition")

    val columnArray = spark.sparkContext.broadcast(columns.split(","))
    val writeDatabase = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.write.database"))
    val writeTable = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.write.table"))

    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:phoenix:bigdata01,bigdata02,bigdata03:2181",
        "driver" -> "org.apache.phoenix.jdbc.PhoenixDriver",
        "dbtable" -> s"(select ${columns} from ${database}.${table} where ${condition} ) as t1"
      )).load().repartition(4).rdd
      .foreachPartition(item => {
        val impala_url = s"jdbc:impala://10.170.3.14:21050/${writeDatabase.value};UseSasl=0;AuthMech=3;UID=qtkj;PWD="
        val conn = ConnectionImpala.getConnection(impala_url)
        conn.setAutoCommit(false)
        var i = 1
        val batchCommitSize = 100
        val upasetStatement = conn.createStatement

        while (item.hasNext) {
          val x = item.next()
          val valuesBuf = new StringBuilder()
          val keysBuf = new StringBuilder()

          for (col <- columnArray.value) {

            keysBuf.append(col.toLowerCase).append(",")
            valuesBuf.append("""'""").append(TypeConversion.strTransform(x.getAs(col))).append("""'""").append(",")
          }
          keysBuf.deleteCharAt(keysBuf.length - 1)
          valuesBuf.deleteCharAt(valuesBuf.length - 1)
          val upsertSql = s"insert into ${writeTable.value} ($keysBuf) values ($valuesBuf)"
          if (upsertSql != null) {
            //插入数据
            upasetStatement.executeUpdate(upsertSql)
            if (i % batchCommitSize == 0) {
              conn.commit()
            }

            i = i + 1
          }
        }
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionImpala.closeConn(conn)
      })

    spark.stop()

  }
}

