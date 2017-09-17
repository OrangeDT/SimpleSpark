package org.training.examples
import org.apache.spark._
import org.apache.spark.util.random
/**
  * Created by Administrator on 2017/6/27.
  */
object SparkPi {
    def main(args:Array[String]): Unit ={
      var masterurl = "yarn-client"

      if(args.length == 1){
        masterurl = args(0)
      }
      val conf = new SparkConf().setMaster(masterurl)setAppName("SparkPi")
      val sc = new SparkContext(conf)
      val slices = if(args.length > 0)args(0).toInt else 2
      val n = 100000 * slices
      val count = sc.parallelize(1 to n,slices).map { i =>
        val x = Math.random() * 2 - 1
        val y = Math.random() * 2 - 1
        if(x * x + y*y < 1) 1 else 0
        }.reduce((a,b) => a + b)
      println("Pi is roughly " + 4.0*count/n)
      sc.stop()
      //master
    }
}



