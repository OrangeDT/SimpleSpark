package org.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Administrator on 2017/7/4.
  * 使用spark完成数据清洗的操作
  * 运行在YARN之上
  */
object SparkStatCleanJobOnYarn {
  def main(args: Array[String]) = {

    if(args.length != 2){
      println("Usage:SparkStatCleanJobOnYarn <inputPath> <outputPath>")
      System.exit(1)
    }
    val Array(inputPath,outputPath) = args
    val spark = SparkSession.builder().getOrCreate()
    val accessRDD = spark.sparkContext.textFile(inputPath)

    //RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(x =>AccessConvertUtil.paraseLog(x)),AccessConvertUtil.struct)

//    accessDF.printSchema()
//    accessDF.show(false)
    //清洗过后，需要写入外部的存储，然后按照某一天进行分区
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
      .save(outputPath)

    spark.stop()
  }
}
