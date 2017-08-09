package org.imooc.firstETL

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat




/**
  * Created by Administrator on 2017/7/4.
  * 日期时间解析工具
  * 出现问题：时间格式上出现了，dd/mmm/yyyy:HH:mm:ss Z;这样是解析不了的，因为有mmm和mm
  */
object DateUtils {
  /**
    *   10/Nov/2016:00:01:02 +0800  ==>>   yyyy-mm--dd HH:MM:SS
    */
    //输入文件格式:10/Nov/2016:00:01:02 +0800
    //simpleDateFormat线程不安全
//    val YYYYMMDDHHMM_TIME_FORMAT =  new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  //目标日期格式:yyyy-mm--dd HH:MM:SS
//  val TARGET_FORMAT = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  /**
    * 获取输入日志时间：long类型
    * time:[10/Nov/2016:00:01:02 +0800]
    */
  def getTime(time:String)  = {
  try {
      //此处的parse和下一个Parse不是一个
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,time.lastIndexOf("]"))).getTime
    }catch{
      case e:Exception =>{
        0l
      }
    }
  }
  /**
    *获取时间：yyyy-MM-dd HH:mm:ss
    * @param time
    */
  def Parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }
  def numString(str:String) :Boolean= {
       str.matches("[0-9]+")
  }
  //测试使用
  def main(args: Array[String]) = {
//    println(Parse("[10/Nov/2016:00:01:02 +0800]"))
//    println(numString("1234556665"))
//    println((numString("3421fjaf")))
  }
}
