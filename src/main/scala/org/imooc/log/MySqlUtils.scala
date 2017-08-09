package org.imooc.log

import java.sql.DriverManager
import java.sql.{Connection, PreparedStatement}

/**
  * Created by Administrator on 2017/7/4.
  * MySql操作工具类，获取数据库连接
  */
object MySqlUtils {
  /**
    * 获取数据库连接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://192.168.56.100:3306/imooc_project?user=root&password=root")
  }

  /**
    * 释放数据库连接等资源
    */
  def release(connection:Connection,pstmt:PreparedStatement) = {
    try {
      if(pstmt != null ){
        pstmt.close()
      }
    }catch {
      case e:Exception => e.printStackTrace( )
    }finally {
      if(connection != null){
        connection.close()
      }
    }
  }
  /**
    * 测试
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
