package cn.qtech.bigdata

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar
import cn.qtech.bigdata.util.SparkReader.readTime
import cn.qtech.bigdata.util.SparkReader.write

import cn.qtech.bigdata.sink.KuduSink
import org.apache.spark.sql.SparkSession

object CBProduceCheckLink extends ProduceCheckLink {

  def main(args: Array[String]): Unit = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
    //source
    val CBProduceTable = "impala::ODS_PRODUCT_PROCESS.AA_REJECT_CB"
 /*   val FPTable = "MESC_TEST_FP_DATA"
    val MODTable = "MESC_TEST_MOD_DATA"*/
    //dest
    val writeTable = "impala::aa_mes.produce_check_cb"

    val spark = SparkSession.builder()
      .appName(CBProduceCheckLink.getClass.getSimpleName)
//      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    spark.sparkContext.setLogLevel("warn")

    val startTime = readTime("/jobs/spark/AAjointest/cBTime.txt",spark)

    println(s"TESTTIME >= '$startTime' and TESTTIME < '$endTime' and IS_VALID = 1")

    createProduceStageView(spark, CBProduceTable, "AA_REJECT_CB")
    createCheckStageView(spark,startTime,endTime)

    // FP MOD 测试表 与生产表关联
    val FPcorrelation = execLinkQuery(spark, "AA_REJECT_CB").cache()

    FPcorrelation.createOrReplaceTempView("tmp1")

    spark.sql(
      """
        |select * from tmp1
      """.stripMargin).show()

    KuduSink.writeDest(FPcorrelation, writeTable)

    FPcorrelation.unpersist()

    spark.stop()

    write(endTime,"/jobs/spark/AAjointest/cBTime.txt")
  }


}
