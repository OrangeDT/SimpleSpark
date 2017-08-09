package org.train.core.movie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/5.
  * 得分最高的10部电影，看过电影最多的前10个人
  */
object TopKMovieAnalyzer {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    var dataPath = "file://G:/云计算大数据/【小象学院】Spark学习课件（董西城）/simplespark/data/ml-1m/"

    if(args.length > 0){
      masterUrl = args(0)
    }else if (args.length > 1){
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl).setAppName("top K movie")
    val sc = new SparkContext(conf)

    /**
      * step 1:创建RDD
      */
    val DATA_PATH = dataPath
    val ratingRdd = sc.textFile(DATA_PATH + "ratings.dat")
    val moviesRdd = sc.textFile(DATA_PATH + "movies.dat")
    /**
      * step 2 :extract colums from RDDs
      */
      //RDD[userid::movieid::rating::timestamp]
      //Rdd[(userid,movieid,score)]
    val ratings = ratingRdd.map(_.split("::")).map(x => (x(0),x(1),x(2))).cache()

    /**
      * step 3 :analyze result
       */

    //users: RDD[(userID, movieID, score)]
    val topKScoreMostMovie = ratings.map{x =>
      (x._2, (x._3.toInt, 1))            //RDD[(movieID,(scoreInt,1))]
    }.reduceByKey { (v1, v2) =>
      (v1._1 + v2._1, v1._2 + v2._2)  //movieID 相同的，scoreInt相加，次数相加RDD[string,(int,int)]
    }.map { x =>
      (x._2._1.toFloat / x._2._2.toFloat, x._1) //计算的是平均分，还有这部电影的评分次数
    }.sortByKey(false)


    /**
      * step 4 transfrom filmID to filename
      */
    //RDD[(movieID,Title)] => map集合
    val movieID2Name = moviesRdd.map(_.split("::")).map(x => (x(0),x(1))).collect().toMap

    /**
      * Map中的if()else()操作的变型
      在Spark中写法是：persons.getOrElse("Spark",1000) //如果persons这个Map中包含有Spark，取出它的值，如果没有，值就是1000。
      */
    topKScoreMostMovie.map(x => (movieID2Name.getOrElse(x._2, null), x._1)).take(20).foreach(println)

    //users: RDD[(userID, movieID, score)]
    val topKmostPerson = ratings.map{ x =>
      (x._1, 1)                     //RDD[(userID,1)]
    }.reduceByKey(_ + _).
      map(x => (x._2, x._1)).//RDD[(count,userID)]
      sortByKey(false).
      take(10).
      foreach(println)


    sc.stop()
  }
}
