package org.train.sparksql


import org.apache.spark._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
/**
  * Created by Administrator on 2017/7/5.
  * RDD和dataframe的转换
  */
object sparkSQLSimpleExample {
  case class User(userID:String,gender:String,age:String,occupation:String,zipcode:String)
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    var datapath = "file://G:/云计算大数据/【小象学院】Spark学习课件（董西城）/simplespark/data/ml-1m/"
    if(args.length > 0){
      masterUrl = args(0)
    }else if(args.length > 1){
      datapath = args(1)
    }

    //在生产环境中，appname和masterURL是通过脚本进行执行的
    val conf = new SparkConf()
      conf.setMaster(masterUrl).setAppName("SparkSQLSimple")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    /**
      * 创建RDD
      * A DataFrame is a Dataset organized into named columns.
      * DataFrame is represented by a Dataset of Rows.
      * DataFrame is simply a type alias of Dataset[Row].
      */

    val DATA_PATH = datapath
    val usersRdd = sc.textFile(DATA_PATH +"users.dat")

    /**
      * method1:通过显示为RDD注入schema,将其变为dataframe
      */
    import spark.implicits._
    val userRDD = usersRdd.map(_.split("::")).map(p => User(p(0),p(1),p(2),p(3),p(4)))
    val userDataFrame = userRDD.toDF()

//    userDataFrame.take(10)
//    userDataFrame.count()

//    /**
//      * method2:通过反射的方式，为RDD注入schema,将其变换为DataFrame
//      */
//    val schemaString = "userID gender age occupation zipcode"
//    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,true)))
//
//    val userRDD2 = usersRdd.map(_.split("::")).map(p =>Row(p(0),p(1).trim,p(2).trim,p(3).trim,p(4).trim))
//    val userDataFrame2 = spark.createDataFrame(userRDD2,schema)
//    userDataFrame2.take(10)
//    userDataFrame2.count()
//
//    userDataFrame2.write.mode(SaveMode.Overwrite).json("/tmp/user.json")
//    userDataFrame2.write.mode(SaveMode.Overwrite).parquet("/tmp/user.parquet")
//
//    /**
//      * 读取json格式的文件
//      */
//    val userJsonDF = spark.read.format("json").load("/tmp/user.json")
//    userJsonDF.take(10)
//
//    /**
//      * 读取json格式的文件
//      */
//    val userJsonDF2 = spark.read.json("/tmp/user.json")
//    userJsonDF2.take(10)
//
//    /**
//      *读取parquet格式的文件
//      */
//    val userParquetDF = spark.read.parquet("/tmp/user.parquet")
//    userParquetDF.take(10)
//    /**
//      * 读取parquet格式的文件
//      */
//    val userParquetDF2 = spark.read.format("parquet").load("/tmp/user.parquet")
//    userParquetDF2.take(10)


    /**
      * 获取RDD，然后通过反射的方式进行转换成DataFrame
      */
    val ratingRdd = sc.textFile(DATA_PATH + "ratings.dat")
    val ratingSchemaString = "UserID movieID Rating Timestamp"
    val ratingSchema = StructType(ratingSchemaString.split(" ").map(fieldName =>StructField(fieldName,StringType,true)))

    val ratingRDD = ratingRdd.map(_.split("::")).map(p => Row(p(0),p(1).trim,p(2).trim,p(3).trim))
    val ratingDataFrame = spark.createDataFrame(ratingRDD,ratingSchema)
    val mergeDataFrame = ratingDataFrame.filter("movieID = 2116").
      join(userDataFrame,"userID").
      select("gender","age").
      groupBy("gender","age").count()

    mergeDataFrame.printSchema()
    mergeDataFrame.show()


    sc.stop()




  }
}
