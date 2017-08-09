package org.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/4.
  * TopN 统计数据
  */
object TopNStatJobOnYarn {
  def main(args: Array[String])  = {


    if(args.length != 2){
      println("Usage:TopNStatJobOnYarn <inputPath> <day>")
    }
    val Array(inputPath,day) = args

    val spark = SparkSession.builder()
      .config("spark.sql.sources.partitionColumnTypeInference.enabled",false)
      .getOrCreate()
    val accessDF = spark.read.format("parquet").load(inputPath)

    StatDAO.deleteDate(day)
//    accessDF.printSchema()
//    accessDF.show()

    videoAccessTopNStat(spark,accessDF,day)

    cityAccessTopNStat(spark,accessDF,day)
    //按照流量进行统计TOPN课程
    videoTrafficTopNStat(spark,accessDF:DataFrame,day)
    spark.stop()

  }

  /**
    *按照流量进行统计
    */
  def videoTrafficTopNStat(spark: SparkSession,accessDF:DataFrame,day:String) = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
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
  def cityAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String): Unit ={
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day","city","cmsId").agg(count("cmsId").as("times"))

//    cityAccessTopNDF.show(false)

    //window函数，在sparksql中的使用
    val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))  //每一个地市的topN
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

  def videoAccessTopNStat(spark: SparkSession, accessDF:DataFrame,day:String) = {

    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

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
