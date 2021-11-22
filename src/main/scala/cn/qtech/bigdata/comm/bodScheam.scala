package cn.qtech.bigdata.comm

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object bodScheam {

  final val schema = StructType(
    StructField("SID", StringType, false) ::
      StructField("AREA", StringType, false) ::
      StructField("COB", StringType, false) ::
      StructField("ID", StringType, true) ::
      StructField("TESTTIME", StringType, true) ::
      StructField("EID", StringType, true) ::
      StructField("DEVICE_NUM", StringType, true) ::
      StructField("STATION", StringType, true) ::
      StructField("MAIN_AA_START", StringType, true) ::
      StructField("GRIPPEROPEN_CNT", StringType, true) ::
      StructField("PATHDATE", StringType, true) ::
      StructField("MAC", StringType, true) ::
      StructField("PART_SPEC", StringType, true) ::
      StructField("BARCODE", StringType, true) ::
      StructField("OPCODE", StringType, true) ::
      StructField("EQUIPMENTNUMBER", StringType, true) ::
      StructField("PROGRAMVER", StringType, true) ::
      StructField("RESULT", StringType, true) ::
      StructField("DEVICEID", StringType, true) ::
      StructField("NGTYPE", StringType, true) ::
      StructField("IS_VALID", StringType, true) ::
      StructField("WO", StringType, true) ::
      StructField("SALES_ORDER", StringType, true) ::
      StructField("EATTRIBUTE1", StringType, true) ::
      StructField("EC_NAME", StringType, true)::
      StructField("STATUS", StringType, true) :: Nil)
}
