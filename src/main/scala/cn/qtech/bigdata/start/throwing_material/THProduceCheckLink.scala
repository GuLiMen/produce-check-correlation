package cn.qtech.bigdata.start.throwing_material

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.qtech.bigdata.sink.KuduSink
import cn.qtech.bigdata.util.SparkReader.{readTime, write}
import org.apache.spark.sql.SparkSession

object THProduceCheckLink extends ProduceCheckLink {

  def main(args: Array[String]): Unit = {

    //定义增量文件结束时间
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)

    //source
    val THProduceTable = "impala::ODS_PRODUCT_PROCESS.AA_REJECT_TH"

    //dest
    val writeTable = "impala::aa_mes.produce_check_th"

    val spark = SparkSession.builder() .appName(THProduceCheckLink.getClass.getSimpleName)
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

    val startTime = readTime("/jobs/spark/AAjointest/tHTime.txt",spark)

    println(s"TESTTIME >= '$startTime' and TESTTIME < '$endTime' and IS_VALID = 1")

    //读取AA生产阶段厂区表数据
    createProduceStageView(spark, THProduceTable, "AA_REJECT_TH")
    createCheckStageView(spark,startTime,endTime)

    // FP MOD 测试表 与生产表关联
    val FPcorrelation = execLinkQuery(spark, "AA_REJECT_TH").cache()
    KuduSink.writeDest(FPcorrelation, writeTable)
    FPcorrelation.unpersist()
    spark.stop()

    //把增量文件结束时间写入文件
    write(endTime,"/jobs/spark/AAjointest/tHTime.txt")
//    val writer = new PrintWriter(new File(tHChengTime))
//    writer.write(endTime)
//    writer.close()
  }


}
