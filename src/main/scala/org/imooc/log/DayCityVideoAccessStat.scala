package org.imooc.log

/**
  * Created by Administrator on 2017/7/4.
  * 统计每一个城市的课程
  */
case class DayCityVideoAccessStat(day:String,cmsId:Long,city:String,times:Long,timesRank:Int)
