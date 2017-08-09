package org.imooc.firstETL

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/7/4.
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {
  def main (args: Array[String]) = {

    //用scala读取文件然后写文件
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///G:/imooclog/access10000.log")
    //测试统计文件所有的行数
//    val count = access.count()
//    println(count)
    access.map(line =>{
      val splits = line.split(" ")
      val ip =splits(0)
      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间
        *
        */
      val time = splits(3) +" " + splits(4)
      val url = splits(11).replaceAll("\"","")
      val traffic = splits(9)


    //  (ip,DateUtils.Parse(time),url,traffic)
      DateUtils.Parse(time) + " " + url + " " + traffic + " " + ip
    }).filter(line => {
      val splits = line.split(" ")
      val url = splits(2)
      val domain = "http://www.imooc.com/"
      if(url.indexOf(domain) != -1 ){
        val cms = url.substring(url.indexOf(domain) + domain.length)
        val cmsTypeId = cms.split("/")

        var cmsType = ""
        var cmsId =  ""
        if (cmsTypeId.length == 2) {
          cmsType = cmsTypeId(0)
          if(cmsType == "video"||cmsType == "article"){
            cmsId = cmsTypeId(1)
            cmsId.matches("[0-9]+")    //正则表达式，是一个数字串，则返回正确，否则错误
          }else false
        }else false
      }else{
        false
      }
    }).map(line => {
      val splits = line.split(" ")
      val YYMMDD =splits(0)
      val HHMMmm = splits(1)
      val url = splits(2)
      val traffic = splits(3)
      val ip = splits(4)


      //  (ip,DateUtils.Parse(time),url,traffic)
      YYMMDD +" "+ HHMMmm+"\t" + url + "\t" + traffic + "\t" + ip
    }).coalesce(1).saveAsTextFile("file:///G:/imooclog/output")
      //take(100).foreach(println)
      //
//    println(count)
    spark.stop()
  }
}
