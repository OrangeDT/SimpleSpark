package org.train.core.movie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/29.
  * 看过“Lord of the Rings,The(1978)”用户年龄性别分布
  * spark core
  */
object movieUserAnalyse {
  def main (args:Array[String]) = {
    //配置信息
    var masterUrl = "local[*]"
    var dataPath = "file://G:/云计算大数据/【小象学院】Spark学习课件（董西城）/simplespark/data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalyse")
    val sc = new SparkContext(conf)

    //step1:create RDD
    val DATA_PATH = dataPath
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"
    val MOVIE_ID = "2016"

    val userRdd = sc.textFile(DATA_PATH + "users.dat")
    val ratingRdd = sc.textFile(DATA_PATH + "ratings.dat")

    //step2:extract colums from RDDS

    //rdd[user::gender::age::occuption::zip-code}
    //rdd[(userid,(gender,age))]
    val users = userRdd.map(_.split("::")).map {
      x => (x(0), (x(1), x(2)))
    }

    //rdd[userid,movieid,ratings,timestamp]
    //rdd[(userid,movieid)]
    val rating = ratingRdd.map(_.split("::"))
    val usermovie = rating.map {
      x => (x(0), x(1))
    }.filter(_._2.equals(MOVIE_ID))

    //step3:joinRDDs
    //userRating:RDD[(userID,(movieID,(gender,age)))]
    val userRating = usermovie.join(users)

    //userRating.take(1).foreach(print)
    val userDistribution = userRating.map {
      x => (x._2._2, 1)
    }.reduceByKey(_ + _)
    userDistribution.foreach(println)

    sc.stop()
    }
  }