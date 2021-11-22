package cn.qtech.bigdata

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import cn.qtech.bigdata.comm.Constants
import cn.qtech.bigdata.comm.Constants.KUDU_MASTER
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


trait ProduceCheckLink_bak {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
/*
    val hours: Long = 1000 * 60 * 60  * 24 //2h
    val beforeNowhours: Long = new Date().getTime - hours
    val beforeNowday: Long = new Date().getTime - hours * 6

    val limitation: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(beforeNowhours)
    val limitationday: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(beforeNowhours)
*/

  val dft: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val todayDate: LocalDateTime = LocalDate.now().atTime(0, 0, 0)
  val today: String = todayDate.format(dft)
  val dayBefore: String = todayDate.plusDays(-1).format(dft)

//  val dayBefore :String= "2021-04-01 11:00:00"

//  println(s"TESTTIME >= '$dayBefore' and TESTTIME < '$today' and IS_VALID = 1")

  /**
   *
   * @param spark
   * @param areaTable AA生产阶段厂区表
   * @param areaView
   */
  def createProduceStageView(spark: SparkSession, areaTable: String, areaView: String): Unit = {
    spark.read.format("kudu")
      .options(Map("kudu.master" -> Constants.KUDU_MASTER,
        "kudu.table" -> areaTable))
      .load.select("area", "cob", "eid", "device_num", "station", "sid", "main_aa_start", "gripperopen_cnt", "pathdate")
      .createOrReplaceTempView(areaView)
  }


  /**
   * 创建检查测试阶段view
   *
   * @param spark
   **/
  def createCheckStageView(spark: SparkSession): Unit = {
    // MESC_TEST_FP_DATA
    spark.read.format("kudu")
      .options(Map("kudu.master" -> KUDU_MASTER,
        "kudu.table" -> "MESC_TEST_FP_DATA"))
      .load.select("id", "MODULEID", "MAC", "TESTTIME", "part_spec", "BARCODE", "OPCODE", "EQUIPMENTNUMBER", "PROGRAMVER", "RESULT", "DEVICEID", "NGTYPE", "IS_VALID","WO")
      .where(s"TESTTIME >= '$dayBefore' and TESTTIME < '$today' and IS_VALID = 1")
        .createOrReplaceTempView("MESC_TEST_FP_DATA")

    // MESC_TEST_MOD_DATA
    spark.read.format("kudu")
      .options(Map("kudu.master" -> KUDU_MASTER,
        "kudu.table" -> "MESC_TEST_MOD_DATA"))
      .load.select("id", "MODULEID", "MAC", "TESTTIME", "part_spec", "BARCODE", "OPCODE", "EQUIPMENTNUMBER", "PROGRAMVER", "RESULT", "DEVICEID", "NGTYPE", "IS_VALID","WO")
      .where(s"TESTTIME >= '$dayBefore' and TESTTIME < '$today' and IS_VALID = 1")
      .createOrReplaceTempView("MESC_TEST_MOD_DATA")

  }


  /**
   * AA阶段与测试阶段表关联
   *
   * @param spark
   * @return
   */
  def execLinkQuery(spark: SparkSession, areaView: String): DataFrame = {
    val correlationResult = spark.sql(
      s"""
         |SELECT a.*,tmpid as id, MAC, TESTTIME, part_spec, BARCODE, OPCODE, EQUIPMENTNUMBER, PROGRAMVER, RESULT, DEVICEID, NGTYPE,IS_VALID,WO
         |from(
         |    SELECT concat('FP_',cast(id as string)) as tmpid,MODULEID, MAC, TESTTIME, part_spec, BARCODE, OPCODE, EQUIPMENTNUMBER, PROGRAMVER, RESULT, DEVICEID, NGTYPE,cast(IS_VALID as tinyint) as IS_VALID,WO
         |    FROM MESC_TEST_FP_DATA
         |    union all
         |    SELECT  concat('MOD_',cast(id as string)) as tmpid,MODULEID, MAC, TESTTIME, part_spec, BARCODE, OPCODE, EQUIPMENTNUMBER, PROGRAMVER, RESULT, DEVICEID, NGTYPE,cast(IS_VALID as tinyint) as IS_VALID,WO
         |    FROM MESC_TEST_MOD_DATA
         |)b
         |join(
         |    SELECT area,cob,eid,device_num,station,sid,main_aa_start,gripperopen_cnt,pathdate FROM (
         |        SELECT area,cob,eid,device_num,station,sid,main_aa_start,gripperopen_cnt,pathdate,
         |        row_number() OVER(PARTITION BY sid ORDER BY main_aa_start desc) num
         |        FROM $areaView
         |    )t WHERE num =1
         |)a
         |  on a.sid =b.MODULEID
         |""".stripMargin)

    correlationResult
  }


}
