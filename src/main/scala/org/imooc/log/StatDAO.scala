package org.imooc.log


import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/4.
  * 统计各个维度统计的DAO操作
  */
object StatDAO {
  /**
    * 批量保存DayVideoAccessStat到数据库
    */
  def insertDayVideoAccessTopN(list:ListBuffer[DayVideoAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySqlUtils.getConnection()

      connection.setAutoCommit(false)   //批处理提交，自动提交关闭
      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      //进行批处理提交
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }


    /**
      * 批量保存每一个城市的课程情况到数据库
      */
    def insertDayCityVideoAccessStat(list:ListBuffer[DayCityVideoAccessStat]) = {
      var connection: Connection = null
      var pstmt: PreparedStatement = null

      try {
        connection = MySqlUtils.getConnection()

        connection.setAutoCommit(false)
        val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?)"
        pstmt = connection.prepareStatement(sql)

        //进行批处理提交
        for (ele <- list) {
          pstmt.setString(1, ele.day)
          pstmt.setLong(2, ele.cmsId)
          pstmt.setString(3, ele.city)
          pstmt.setLong(4,ele.times)
          pstmt.setInt(5,ele.timesRank)

          pstmt.addBatch()
        }
        pstmt.executeBatch() //执行批量处理
        connection.commit() //手工提交
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        MySqlUtils.release(connection, pstmt)
      }
    }

  /**
    * 按照流量统计课程
    * @param list
    */
  def insertDayVideoTrafficsAccessStat(list:ListBuffer[DayVideoTrafficStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySqlUtils.getConnection()

      connection.setAutoCommit(false)
      val sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      //进行批处理提交
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)

        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }

  /**
    * 统计每一个省市的流量
    * @param list
    */
  def insertCityTrafficStat(list:ListBuffer[CityTrafficStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySqlUtils.getConnection()

      connection.setAutoCommit(false)
      val sql = "insert into city_traffic_stat(day,city,traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      //进行批处理提交
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setString(2, ele.city)
        pstmt.setLong(3, ele.traffics)

        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, pstmt)
    }
  }


  /**
    * 删除指定天数的数据
    * |
    * day_video_access_topn_stat
      day_video_city_access_topn_stat
      day_video_traffics_topn_stat
    * @param day
    */
  def deleteDate(day:String) ={
    val tables = Array("day_video_access_topn_stat",
      "day_video_city_access_topn_stat",
      "day_video_traffics_topn_stat")
    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MySqlUtils.getConnection()

      for(table <- tables){
        val deleteSQL = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1,day)
        pstmt.executeUpdate()
      }

    }catch {
      case e :Exception => e.printStackTrace()
    }finally {
      MySqlUtils.release(connection,pstmt)
    }
  }
}
