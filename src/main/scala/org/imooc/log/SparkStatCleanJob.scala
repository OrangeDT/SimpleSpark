package org.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Administrator on 2017/7/4.
  * 使用spark完成数据清洗的操作
  * 运行在YARN之上
  */
object SparkStatCleanJob {
  def main(args: Array[String]) = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

   // val accessRDD = spark.sparkContext.textFile("file:///G:/云计算大数据/慕课网日志分析/镜像文件/access.log")
    val accessRDD = spark.sparkContext.textFile("file:///G:/imooclog/output/part-00000")
    //RDD => DF，为什么要转换到RDD，因为可能的是，不支持转换.log的文件
    val accessDF = spark.createDataFrame(accessRDD.map(x =>AccessConvertUtil.paraseLog(x)),AccessConvertUtil.struct)

//    accessDF.printSchema()
//    accessDF.show(1000)
    //清洗过后，需要写入外部的存储，然后按照某一天进行分区
    //coalesce:输出分区文件个数
//    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
//      .save("file:///E:/JavaBigDataProject/imooclogclean")

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
      .save("file:///G:/imooclog/secondoutput")

    spark.stop()
  }
}
