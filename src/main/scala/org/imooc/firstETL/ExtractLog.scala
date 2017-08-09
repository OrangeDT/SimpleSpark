package org.imooc.firstETL

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/7/7.
  * 由于日志文件5个G所以截取一部分日志文件进行分析
  */
object ExtractLog {
  //用scala读取文件然后写文件
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///G:/云计算大数据/慕课网日志分析/镜像文件/access.20161111.log")
    access.take(10).foreach(println)
    val stringC = access.take(10000)

    val writer = new PrintWriter(new File("access10000.log" ))

    for( x <- stringC){
      writer.write(x)
      writer.write(0x0d)
      writer.write(0x0a)
    }

    writer.close()
    spark.stop()
  }

}
