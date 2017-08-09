package org.imooc.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType,StringType}

/**
  * Created by Administrator on 2017/7/4.
  * 访问日志转换（输入 =>  输出）
  */
object AccessConvertUtil {
  //定义的是输出的字段
  val struct = StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),

      StructField("time",StringType),
      StructField("day",StringType)
    )
  )
  /**
    * 根据输入的每一行信息转换成输出的样式
    */
  def paraseLog(log:String) = {

    try {
      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      var city = IpUtils.getCity(ip)
      if(city == "全球"||city == "全球 "){
        city = "全球"
      }

      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      //这个Row里面的字段要和结构体里面的字段对应上
      //spark sql里面的一个类型
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    }catch {
      case e:Exception => Row(0)
    }
  }
}
