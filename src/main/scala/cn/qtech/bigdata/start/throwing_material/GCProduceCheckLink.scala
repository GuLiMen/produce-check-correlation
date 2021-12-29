package cn.qtech.bigdata.start.throwing_material

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.qtech.bigdata.sink.KuduSink
import cn.qtech.bigdata.util.SparkReader.{readTime, write}
import org.apache.spark.sql.SparkSession

object GCProduceCheckLink extends ProduceCheckLink {

  def main(args: Array[String]): Unit = {

    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)

    //source
    val GCProduceTable = "impala::ODS_PRODUCT_PROCESS.AA_REJECT_GC"

    //dest
    val writeTable = "impala::aa_mes.produce_check_gc"

    val spark = SparkSession.builder().appName(GCProduceCheckLink.getClass.getSimpleName)

       .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice")
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    var startTime = readTime("/jobs/spark/AAjointest/gCTime.txt",spark)

    println(s"TESTTIME >= '$startTime' and TESTTIME < '$endTime' and IS_VALID = 1")

   // spark.sparkContext.setLogLevel("warn")
    createProduceStageView(spark, GCProduceTable, "AA_REJECT_GC")
    createCheckStageView(spark,startTime,endTime)


    // FP MOD 测试表 与生产表关联
    val FPcorrelation = execLinkQuery(spark, "AA_REJECT_GC")
//    FPcorrelation.show()
    KuduSink.writeDest(FPcorrelation, writeTable)
    FPcorrelation.unpersist()

    spark.stop()

    write(endTime,"/jobs/spark/AAjointest/gCTime.txt")

//    val writer = new PrintWriter(new File(guChengTime))
//    writer.write(endTime)
//    writer.close()
  }
}
