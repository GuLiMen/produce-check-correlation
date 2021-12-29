package cn.qtech.bigdata.util

import cn.qtech.bigdata.comm.Constants.RECEIVE_EMAIL
import java.sql.{Connection, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import cn.qtech.bigdata.comm.SendEMailWarning

import scala.collection.mutable.ListBuffer

object ImpalaJDBC {


  /**
   * 根据 SQL 插入数据
   *
   * @param Sql
   */
  def insertData(conn: Connection, Sql: String): Unit = {
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(Sql)
      conn.commit()
    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"data-etl run Class  ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")

      }
    }
  }

  /**
   * 根据 SQL 建表/添加字段
   *
   * @param Sql
   */
  def creOrAlterTable(conn: Connection, Sql: String): Unit = {
    try {
      val stmt = conn.createStatement()
      stmt.executeUpdate(Sql)
      conn.commit()
    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"data-etl run Class  ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} ${e.getStackTrace} \n")
      }
    }
  }

  /**
   * 返回查询结果
   *
   * @param sql
   * @return
   */
  def selectResult(conn: Connection, sql: String): ResultSet = {
    var rs: ResultSet = null
    try {
      val stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"data-etl run Class  ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} ${e.getStackTrace}\n")
      }
    }
    rs
  }

  /**
   * 遍历结果集
   *
   * @param rs
   * @return
   */
  def foreachResultSet(rs: ResultSet) = {
   // println("============foreachResultSet==============")
    val colSeq = ListBuffer[String]()
    while (rs.next()) {
      colSeq += rs.getString("COLUMN_NAME")
    }
    colSeq
  }
}