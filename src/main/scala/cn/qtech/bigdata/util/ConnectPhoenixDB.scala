package cn.qtech.bigdata.util

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

object ConnectPhoenixDB {
  /**
   * 获取 phoenix 连接
   *
   * @param url
   * @return
   */
  def getConnection(url: String): Connection = {
    var conn: Connection = null
    try {
   //   println("------------before connect----------------------")
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      conn = DriverManager.getConnection(url)
   //   println(s"------------get connect ${conn.isClosed}----------------------")

    } catch {
      case e: Exception => {
        println(e.getMessage)
      }

    }
    conn
  }


  /**
   * 关闭连接
   *
   * @param conn
   */
  def closeConn(conn: Connection): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    }
  }
}
