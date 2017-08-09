package org.train.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/7/6.
  */
object sparkSessionTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    var dataPath = "file:///G:/云计算大数据/【小象学院】Spark学习课件（董西城）/simplespark/data/"
    val people = spark.read.json(dataPath + "test.json")
    people.show()
    spark.stop()
  }
}
