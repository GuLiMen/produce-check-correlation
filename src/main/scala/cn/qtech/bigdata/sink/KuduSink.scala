package cn.qtech.bigdata.sink


import cn.qtech.bigdata.comm.Constants.KUDU_MASTER
import cn.qtech.bigdata.util.TypeConversion
import org.apache.kudu.client.{KuduClient, KuduException, SessionConfiguration}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

object KuduSink {
  val LOG = LoggerFactory.getLogger(KuduSink.getClass.getName)

  def writeDest(result: DataFrame, destTable: String): Unit = {
    result
      .rdd
      .coalesce(10)
      .foreachPartition(item => {
      val kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTER).build
      val kuduTable = kuduClient.openTable(destTable)
      val kuduSession = kuduClient.newSession
      kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)

      while (item.hasNext) {
        val x: Row = item.next()
        val insert = kuduTable.newUpsert()
        val row = insert.getRow()

        for (f <- x.schema.fields) {
          val colName = f.name
          val dataType = f.dataType.toString
          if ("StringType".equals(dataType)) {
            row.addString(colName.toLowerCase(), TypeConversion.strTransform(x.getAs(colName)))
          } else if ("ByteType".equals(dataType)) {
            row.addByte(colName.toLowerCase(), TypeConversion.intTransform(x.getAs(colName)).toByte)
          } else if ("IntegerType".equals(dataType)) {
            row.addInt(colName.toLowerCase(), TypeConversion.intTransform(x.getAs(colName)))
          } else {
            println("未指定的数据类型 !!")
          }
        }
        try {
          val response = kuduSession.apply(insert)
          //检查插入数据是否失败
          if (response != null && kuduSession.countPendingErrors != 0) {
            println("response写入失败 !! ->  " + x.toString())
          }
        } catch {
          case e: KuduException => {
            println("数据写入失败 !! ->  " + x.toString())
            LOG.error("数据写入失败 !! ")
          }
        }
      }
    })

  }
}
