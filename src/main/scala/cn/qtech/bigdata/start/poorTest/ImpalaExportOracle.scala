package cn.qtech.bigdata.start.poorTest

import java.sql.DriverManager
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import cn.qtech.bigdata.comm.Constants.KUDU_MASTER
import cn.qtech.bigdata.start.TypeConversion
import org.apache.spark.sql.SparkSession

object ImpalaExportOracle {
  def main(args: Array[String]): Unit = {
    //source
    val readTH = "impala::ODS_PRODUCT_PROCESS.AA_REJECT_TH"
    val readGC = "impala::ODS_PRODUCT_PROCESS.AA_REJECT_GC"
    val readCB = "impala::ODS_PRODUCT_PROCESS.AA_REJECT_CB"

    //condition
    val dft: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val todayDate: LocalDateTime = LocalDate.now().atTime(0, 0, 0)
    val today: String = todayDate.format(dft)
    val dayBefore: String = todayDate.plusDays(-1).format(dft)
    val condition = s"parse_time >= '$dayBefore' and parse_time < '$today' "
    println(condition)
    val spark = SparkSession.builder().appName(ImpalaExportOracle.getClass.getSimpleName)
      //   .master("local[*]")
      .getOrCreate()

    val th = spark.read.format("kudu")
      .options(Map("kudu.master" -> KUDU_MASTER,
        "kudu.table" -> readTH))
      .load.select("cob", "eid", "device_num", "sid", "uv_mtf_check2_fail_cnt", "saveoc_fail_cnt", "main_aa_start")
      .where(s"$condition")

    val gc = spark.read.format("kudu")
      .options(Map("kudu.master" -> KUDU_MASTER,
        "kudu.table" -> readGC))
      .load.select("cob", "eid", "device_num", "sid", "uv_mtf_check2_fail_cnt", "saveoc_fail_cnt", "main_aa_start")
      .where(s"$condition")

    val cb = spark.read.format("kudu")
      .options(Map("kudu.master" -> KUDU_MASTER,
        "kudu.table" -> readCB))
      .load.select("cob", "eid", "device_num", "sid", "uv_mtf_check2_fail_cnt", "saveoc_fail_cnt", "main_aa_start")
      .where(s"$condition")

    th.union(gc).union(cb)
      .rdd.coalesce(15)
      .foreachPartition(item => {
        Class.forName("oracle.jdbc.OracleDriver")
        val conn = DriverManager.getConnection("jdbc:oracle:thin:@10.170.1.57:1521/qtsoft", "bigdata", "bigdata2021")
        val upasetStatement = conn.createStatement

        while (item.hasNext) {
          val x = item.next()
          val valuesBuf = new StringBuilder()
          val keysBuf = new StringBuilder()
          var flag = false
          for (f <- x.schema.fields) {
            val colName = f.name
            if ("cob".equals(colName)) {
              keysBuf.append("WORKSHOP_CODE").append(",")
              valuesBuf.append("""'""").append(TypeConversion.strTransform(x.getAs(colName))).append("""'""").append(",")
            } else if ("eid".equals(colName)) {
              keysBuf.append("EQID").append(",")
              valuesBuf.append("""'""").append(TypeConversion.strTransform(x.getAs(colName))).append("""'""").append(",")
            } else if ("device_num".equals(colName)) {
              keysBuf.append("PART_SPEC").append(",")
              valuesBuf.append("""'""").append(TypeConversion.strTransform(x.getAs(colName))).append("""'""").append(",")
            } else if ("uv_mtf_check2_fail_cnt".equals(colName) || "saveoc_fail_cnt".equals(colName)) {
              val var1 = TypeConversion.intTransform(x.getAs(colName))
              if (var1 > 0) {
                flag = true
                keysBuf.append("NGTYPE").append(",")
                valuesBuf.append("""'""").append(colName.replace("_fail_cnt", "")).append("""'""").append(",")
              }

            } else if ("sid".equals(colName)) {
              keysBuf.append("MODULEID").append(",")
              valuesBuf.append("""'""").append(TypeConversion.strTransform(x.getAs(colName))).append("""'""").append(",")
            } else if ("main_aa_start".equals(colName)) {
              keysBuf.append("MDATE").append(",")
              valuesBuf.append("to_date('").append(x.getAs[String](colName)).append("','yyyy-mm-dd hh24:mi:ss'))")
            }
          }
          if (flag) {
            keysBuf.deleteCharAt(keysBuf.length - 1)
            valuesBuf.deleteCharAt(valuesBuf.length - 1)
            val insertSql = s"insert into CKMES.T_AA_RESULT (${keysBuf.toString()}) values (${valuesBuf.toString()})"
            // println(insertSql)
            upasetStatement.executeUpdate(insertSql)
            conn.commit()
          }


        }

      })

    //建议使用oracle.jdbc.OracleDriver类，不建议使用oracle.jdbc.driver.OracleDriver。从9.0.1开始的每个release都推荐使用oracle.jdbc
    /*    val aaa = spark.read.format("jdbc")
          .options(Map(
            "url" -> s"jdbc:oracle:thin:@10.170.1.57:1521/qtsoft",
            "driver" -> "oracle.jdbc.OracleDriver",
            "dbtable" -> "CKMES.T_AA_RESULT",
            "user" -> "bigdata",
            "password" -> "bigdata2021"
          )).load().rdd.foreach(println(_))*/
  }
}
