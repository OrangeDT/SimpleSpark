package org.train.core.movie

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * Created by Administrator on 2017/7/5.
  * 目标：18-40周岁最爱看的十部电影
  */
object PopularMovieAnalyzer {
  def main (args:Array[String]) = {
    //配置信息
    var masterUrl = "local[2]"
    var dataPath = "file://G:/云计算大数据/【小象学院】Spark学习课件（董西城）/simplespark/data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("PopularMovieAnalyzer")
    val sc = new SparkContext(conf)


    /**
      * step 1:Create RDDS
      */
    val DATA_PATH = dataPath
    val USER_AGE_B = "18"
    val USER_AGE_T = "40"


    val usersRdd = sc.textFile(DATA_PATH + "users.dat")
    val moviesRdd = sc.textFile(DATA_PATH + "movies.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")


    /**
      * step 2: extract colums from Rdds
      */
    //users: RDD[(userID, age)]
    val users = usersRdd.map(_.split("::")).map(x => (x(0),x(2))).filter(_._2.>(USER_AGE_B)).filter(_._2.<=(USER_AGE_T))

    users.foreach(println)
    //Array[String]
    //collect算子相当于toArray
    val userlist = users.map(_._1).collect()



    //broadcast,这个广播变量什么作用
    //广播变量每个executor一份
    //HashSet()++userlist:添加++后面集合的元素，创建一个新的hashset，hashset是要删除重复的元素的
    val userSet = HashSet() ++ userlist
    val broadcastUserSet = sc.broadcast(userSet)


    /**
      * step 3 map_side join RDDS
      */
      //RDD[(userID,movieID)]
      //为什么要有广播变量呢，因为用户列表中有没有看过电影的，只是注册了一个账号
    val topKmovies = ratingsRdd.map(_.split("::")).map{ x =>
      (x(0), x(1))
    }.filter { x =>
      broadcastUserSet.value.contains(x._1)        //RDD[(userID,movieID,)],从评分的表中获取前面符合年龄限制的
    }.map{ x=>                                      //RDD[(movieID,1)]
      (x._2, 1)
    }.reduceByKey(_ + _).map{ x =>                //RDD[(movieID,count)]=>RDD[(count,movieID)]
      (x._2, x._1)
    }.sortByKey(false).map{ x=>                //降序排列，RDD[(movieID,count)]
      (x._2, x._1)
    }.take(10)


    /**
      * step 4 transfrom filmID to filename
      */
      //RDD[(movieID,Title)] => map集合
    val movieID2Name = moviesRdd.map(_.split("::")).map(x => (x(0),x(1))).collect().toMap

    /**
      * Map中的if()else()操作的变型
      在Spark中写法是：persons.getOrElse("Spark",1000) //如果persons这个Map中包含有Spark，取出它的值，如果没有，值就是1000。
      */
    topKmovies.map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)

    sc.stop()
  }
}
