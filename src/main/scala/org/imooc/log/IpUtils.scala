package org.imooc.log

import com.ggstar.util.ip.IpHelper

/**
  * Created by Administrator on 2017/7/8.
  * IP解析工具类
  */
object IpUtils {
  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }


  //1.28.64.0:内蒙古包头129.10.9.110,71.17.103.212 131.107.0.115
  // 71.17.103.212   全球 |
  // 103.204.172.213 全球|
  def main(args: Array[String]) = {
    println(getCity("71.17.103.212")+"|")
    println(getCity("103.204.172.213")+"|")

  }
}
