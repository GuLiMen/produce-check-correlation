package cn.qtech.bigdata.comm

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark

object Constants {
  final val KUDU_MASTER = "bigdata01:7051,bigdata02:7051,bigdata03:7051"

  final val JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver"
  final val CONNECTION_URL = "jdbc:impala://10.170.3.15:21050/erp_job;UseSasl=0;AuthMech=3;UID=qtkj;PWD=qt_qt"

  final val gcView="AA_REJECT_GC"
  final val thView="AA_REJECT_TH"
  final val cbView="AA_REJECT_CB"

  //记录增量时间结束linux（10.170.3.11）文件位置
  final val guChengTime = "/data/workspace/project/file/aa_Test_Lot_file/gCTime.txt"
  final val tHChengTime = "/data/workspace/project/file/aa_Test_Lot_file/tHTime.txt"
  final val cBChengTime = "/data/workspace/project/file/aa_Test_Lot_file/cBTime.txt"


  val now: Date = new Date()
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  final val date = dateFormat.format(now)

}