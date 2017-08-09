package org.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/4.
  * TopN 统计数据--优化版本
  */
object TopNStatJobOPT {
  def main(args: Array[String])  = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled",false)
      .master("local[2]").getOrCreate()
    val accessDF = spark.read.format("parquet").load("file:///E:/JavaBigDataProject/imooclogclean")
//    val accessDF = spark.read.format("parquet").load("file:///G:/imooclog/secondoutput")
    import spark.implicits._
    val day = "20170511"
//    val day = "20161110"
    val commonDF = accessDF.filter($"day" === day && $"cmsType" === "video")

    commonDF.cache()

    StatDAO.deleteDate(day)
//    accessDF.printSchema()
//    accessDF.show(false)

    //按照访问次数统计TOPN课程
    videoAccessTopNStat(spark,commonDF)

    //统计每一个省市的TOPN课程
    cityAccessTopNStat(spark,commonDF)
    //按照流量进行统计TOPN课程
    videoTrafficTopNStat(spark,commonDF)

    //统计每一个省市的流量
//    cityTrafficTopNStat(spark,commonDF)

   commonDF.unpersist(true)
    spark.stop()

  }

  /**
    * 统计每一省市的流量
    */
  def cityTrafficTopNStat(spark: SparkSession,commonDF:DataFrame) = {
    import spark.implicits._
    val cityAccessTopNDF = commonDF.groupBy("day","city").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
    //      .show(false)

    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[CityTrafficStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val city = info.getAs[String]("city")
          val traffics = info.getAs[Long]("traffics")

          list.append(CityTrafficStat(day, city, traffics))

        })
        StatDAO.insertCityTrafficStat(list)
      })
    }catch  {
      case e : Exception => e.printStackTrace()
    }


  }
  /**
    *按照流量进行统计最高的课程
    */
  def videoTrafficTopNStat(spark: SparkSession,commonDF:DataFrame) = {
    import spark.implicits._
    val cityAccessTopNDF = commonDF.groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
//      .show(false)

    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficStat(day, cmsId, traffics))

        })
        StatDAO.insertDayVideoTrafficsAccessStat(list)
      })
    }catch  {
      case e : Exception => e.printStackTrace()
    }


  }
  /**
    * 按照地市进行统计TopN课程
    */
  def cityAccessTopNStat(spark:SparkSession,commonDF:DataFrame): Unit ={
    val cityAccessTopNDF = commonDF.groupBy("day","city","cmsId").agg(count("cmsId").as("times"))

//    cityAccessTopNDF.show(false)

    //window函数，在sparksql中的使用
    val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc))as("times_rank")
    ).filter("times_rank <= 3")
      //.show(false)  //每一个地市Top3

    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city,times,timesRank))

        })
        StatDAO.insertDayCityVideoAccessStat(list)
      })
    }catch  {
      case e : Exception => e.printStackTrace()
    }


  }
  /**
    * 最受欢迎的topN课程
    */
  def videoAccessTopNStat(spark: SparkSession, commonDF:DataFrame) = {

    import spark.implicits._
    val videoAccessTopNDF = commonDF.groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

//    videoAccessTopNDF.show(false)


    /**
      * 采用sql语句的方式进行
      */
//    accessDF.createOrReplaceTempView("access_logs")
//    val videoAccessTopNDF = spark.sql("select day,cmsId,count(1) as times from access_logs "+
//    "where day = '20170511' and cmsType = 'video' " +
//      "group by day,cmsId order by times desc")
//    videoAccessTopNDF.show(false)


    /**
      * 将统计结果写入到数据库中
      */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))

        })
        StatDAO.insertDayVideoAccessTopN(list)
      })
    }catch  {
      case e : Exception => e.printStackTrace()
    }
  }
}
