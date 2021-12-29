package cn.qtech.bigdata.start.poorTest


import cn.qtech.bigdata.comm.Constants.KUDU_MASTER
import cn.qtech.bigdata.comm.bodScheam.schema
import cn.qtech.bigdata.util.badTestStatistics
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object badTestTh extends badTestStatistics{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("badTest")
//      .master("local[*]")
      .config("spark.port.maxRetries","500")
      .config("spark.debug.maxToStringFields", "200")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val kuduContext = new KuduContext(KUDU_MASTER, sc)

    val ads_bad_test_statistics = "ADS_BAD_TEST_STATISTICS"

    kuduTable(spark,"ADS_BAD_TEST_STATISTICS","ads_bad_test_statistics")
    kuduTable(spark,"MESC_B_WORKSHOP","mesc_b_workshop")
    kuduTable(spark,"MESC_B_ERRORCODE","mesc_b_errorcode")
    kuduTable(spark,"MESC_TEST_MSR","mesc_test_msr")
    kuduTable(spark,"MESC_TEST_SNCODE_CHECKRESULT","mesc_test_sncode_checkresult")
    kuduTable(spark,"MESC_I_WO","mesc_i_wo")
    kuduTable(spark,"impala::aa_mes.produce_check_th","produce_check_th")
    kuduTable(spark,"impala::aa_mes.produce_check_gc","produce_check_gc")
    kuduTable(spark,"impala::aa_mes.produce_check_cb","produce_check_cb")

//    val result: DataFrame = test_statistics(spark,"produce_check_th")

    val result: DataFrame = test_statistics_TH(spark)

    spark.createDataFrame(result.rdd,schema)
      .write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", ads_bad_test_statistics)
      .save()

  }

}
