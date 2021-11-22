package cn.qtech.bigdata.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import cn.qtech.bigdata.comm.Constants.KUDU_MASTER

trait badTestStatistics {

  def kuduTable(spark: SparkSession, areaTable: String, areaView: String): Unit = {
    spark.read.format("kudu")
      .options(Map("kudu.master" -> KUDU_MASTER,
        "kudu.table" -> areaTable))
      .load
      .createOrReplaceTempView(areaView)
  }


  //sql与逻辑是丁辉提供
  def test_statistics_GC(spark: SparkSession): DataFrame = {
    val df: DataFrame = spark.sql(
      s"""
         |select
         |cast(sid  as string),
         |'GuCheng' as area,
         |case when COB="COB5" then "COB2" ELSE COB end as cob,
         |cast(t.id               as string),
         |cast(t.testtime         as string),
         |cast(t.eid              as string),
         |cast(t.device_num       as string),
         |cast(t.station          as string),
         |cast(t.main_aa_start    as string),
         |cast(t.gripperopen_cnt  as string),
         |cast(t.pathdate         as string),
         |cast(t.mac              as string),
         |cast(t.part_spec        as string),
         |cast(t.barcode          as string),
         |cast(t.opcode           as string),
         |cast(t.equipmentnumber  as string),
         |cast(t.programver       as string),
         |cast(t.result           as string),
         |cast(t.deviceid         as string),
         |cast(t.ngtype           as string),
         |cast(t.is_valid         as string),
         |cast(t.wo               as string),
         |cast(wo.sales_order     as string),
         |cast(shop.eattribute1   as string),
         |cast(code.ec_name   as string),
         |case when
         |    t.result = 'NG' then '1'
         |    else
         |        case when sn.sncode is not null
         |        then '1'  else '0'
         |    end
         |end as status
         |from (
         |SELECT DISTINCT t.*
         |		FROM produce_check_gc t
         |		INNER JOIN (
         |					SELECT sid as sid_, max(testtime) as testtime_
         |					FROM  produce_check_gc
         |					WHERE is_valid =1
         |					and area = 'GuCheng'
         |					GROUP BY sid
         |
         |				) ta ON t.sid = ta.sid_ AND t.testtime = ta.testtime_
         |				)t
         |
         |		LEFT JOIN mesc_i_wo wo on wo.wo_code = t.wo
         |		LEFT JOIN mesc_b_workshop shop on shop.workshop_code = wo.sales_order
         |		LEFT JOIN mesc_b_errorcode code on code.is_valid =1 and code.ec_code = t.ngtype
         |		LEFT JOIN mesc_test_msr msr on msr.moduleid = t.sid
         |		LEFT JOIN mesc_test_sncode_checkresult  sn on sn.is_valid =1 and sn.sncode = msr.sncode
         |
         |		where t.is_valid = 1
         |			and (t.`result` = 'NG' OR ( t.`result` ='OK' and sn.sncode <>'' and sn.sncode is not null))
         |			and t.testtime > concat(substr(cast(date_add(now(),-3) as string),1,10),' 08:00:00') and t.testtime < substr(cast(now() as string) ,1,19)
      """.stripMargin)

    df
  }


  def test_statistics_TH(spark: SparkSession): DataFrame = {
    val df: DataFrame = spark.sql(
      s"""
         |select
         |cast(sid  as string),
         |'TaiHong' as area,
         |case when COB="COB5" then "COB2" ELSE COB end as cob,
         |cast(t.id               as string),
         |cast(t.testtime         as string),
         |cast(t.eid              as string),
         |cast(t.device_num       as string),
         |cast(t.station          as string),
         |cast(t.main_aa_start    as string),
         |cast(t.gripperopen_cnt  as string),
         |cast(t.pathdate         as string),
         |cast(t.mac              as string),
         |cast(t.part_spec        as string),
         |cast(t.barcode          as string),
         |cast(t.opcode           as string),
         |cast(t.equipmentnumber  as string),
         |cast(t.programver       as string),
         |cast(t.result           as string),
         |cast(t.deviceid         as string),
         |cast(t.ngtype           as string),
         |cast(t.is_valid         as string),
         |cast(t.wo               as string),
         |cast(wo.sales_order     as string),
         |cast(shop.eattribute1   as string),
         |cast(code.ec_name   as string),
         |case when
         |    t.result = 'NG' then '1'
         |    else
         |        case when sn.sncode is not null
         |        then '1'  else '0'
         |    end
         |end as status
         |from (
         |SELECT DISTINCT t.*
         |		FROM produce_check_th t
         |		INNER JOIN (
         |					SELECT sid as sid_, max(testtime) as testtime_
         |					FROM  produce_check_th
         |					WHERE is_valid =1
         |					and area = 'TaiHong'
         |					GROUP BY sid
         |
         |				) ta ON t.sid = ta.sid_ AND t.testtime = ta.testtime_
         |				)t
         |
         |		LEFT JOIN mesc_i_wo wo on wo.wo_code = t.wo
         |		LEFT JOIN mesc_b_workshop shop on shop.workshop_code = wo.sales_order
         |		LEFT JOIN mesc_b_errorcode code on code.is_valid =1 and code.ec_code = t.ngtype
         |		LEFT JOIN mesc_test_msr msr on msr.moduleid = t.sid
         |		LEFT JOIN mesc_test_sncode_checkresult  sn on sn.is_valid =1 and sn.sncode = msr.sncode
         |
         |		where t.is_valid = 1
         |			and (t.`result` = 'NG' OR ( t.`result` ='OK' and sn.sncode <>'' and sn.sncode is not null))
         |			and t.testtime > concat(substr(cast(date_add(now(),-3) as string),1,10),' 08:00:00') and t.testtime < substr(cast(now() as string) ,1,19)
      """.stripMargin)

    df
  }


  def test_statistics_G2(spark: SparkSession): DataFrame = {
    val df: DataFrame = spark.sql(
      s"""
         |select
         |cast(sid  as string),
         |'GuCheng2' as area,
         |cob,
         |cast(t.id               as string),
         |cast(t.testtime         as string),
         |cast(t.eid              as string),
         |cast(t.device_num       as string),
         |cast(t.station          as string),
         |cast(t.main_aa_start    as string),
         |cast(t.gripperopen_cnt  as string),
         |cast(t.pathdate         as string),
         |cast(t.mac              as string),
         |cast(t.part_spec        as string),
         |cast(t.barcode          as string),
         |cast(t.opcode           as string),
         |cast(t.equipmentnumber  as string),
         |cast(t.programver       as string),
         |cast(t.result           as string),
         |cast(t.deviceid         as string),
         |cast(t.ngtype           as string),
         |cast(t.is_valid         as string),
         |cast(t.wo               as string),
         |cast(wo.sales_order     as string),
         |cast(shop.eattribute1   as string),
         |cast(code.ec_name   as string),
         |case when
         |    t.result = 'NG' then '1'
         |    else
         |        case when sn.sncode is not null
         |        then '1'  else '0'
         |    end
         |end as status
         |from (
         |SELECT DISTINCT t.*
         |		FROM produce_check_cb t
         |		INNER JOIN (
         |					SELECT sid as sid_, max(testtime) as testtime_
         |					FROM  produce_check_cb
         |					WHERE is_valid =1
         |					and area = 'GuCheng2'
         |					GROUP BY sid
         |
         |				) ta ON t.sid = ta.sid_ AND t.testtime = ta.testtime_
         |				)t
         |
         |		LEFT JOIN mesc_i_wo wo on wo.wo_code = t.wo
         |		LEFT JOIN mesc_b_workshop shop on shop.workshop_code = wo.sales_order
         |		LEFT JOIN mesc_b_errorcode code on code.is_valid =1 and code.ec_code = t.ngtype
         |		LEFT JOIN mesc_test_msr msr on msr.moduleid = t.sid
         |		LEFT JOIN mesc_test_sncode_checkresult  sn on sn.is_valid =1 and sn.sncode = msr.sncode
         |
         |		where t.is_valid = 1
         |			and (t.`result` = 'NG' OR ( t.`result` ='OK' and sn.sncode <>'' and sn.sncode is not null))
         |			and t.testtime > concat(substr(cast(date_add(now(),-3) as string),1,10),' 08:00:00') and t.testtime < substr(cast(now() as string) ,1,19)
      """.stripMargin)

    df
  }

}
