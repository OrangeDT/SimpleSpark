package org.training.examples

/**
  * Created by Administrator on 2017/7/5.
  */
object simpleScala {
  def sum(f:Int => Int)(a:Int)(b:Int):Int = {
    @annotation.tailrec
    def loop(n:Int,acc:Int):Int =
      if (n > b) {
        println(s"n=${n},acc=${acc}")
        acc
      }
      else {
        println(s"n=${n},acc=${acc}")
        loop(n+1, acc+f(n))
      }
    loop(a,0)
  }

  /**
    * 测试map 和flatMap
    */
  def mapTest() = {
    val a = List(1,2,3,4,5)
    val b = List(a,List(6,1,3,4))

    val c = b.map(_.filter(_%2 == 0))
   // val c = b.map{x => x.filter(_%2 == 0)}

    val d = b.flatMap(_.filter(_%2 ==0))
    //val d = b.flatMap{x => x.filter(_%2 == 0)}
    println(c)

    println(d)

  }

  /**
    * 快速排序
    *
    */
  def qSort(a:List[Int]):List[Int] = {
    if (a.length < 2) a
    else qSort(a.filter(a.head > _))++     //++用于拼接，通过高阶函数，通过递归，快速实现排序
      a.filter(a.head == _)++
      qSort(a.filter(a.head<_))
  }


  def rectangle(length: Double ) = (height:Double) =>(length +height) *2

  def main(args: Array[String]): Unit = {
//    println(sum(x => x)(1)(5))
//    mapTest()
//    val l = qSort(List(6,3,5,8,2,5,6))
//    println(l)
//    val spark = SparkSession.builder().master("local[2]").appName("simple").getOrCreate()
//    val visit = spark.sparkContext.parallelize(List(("index.html","1.2.3.4"),("about.html","3,4,5,6"),("index.html","1.3.3.1"),("hello.html","1,2,3,4")),2);
//    val page = spark.sparkContext.parallelize(List(("index.html","home"),("about.html","about"),("hi.html","2.3.3.3")),2);
//
//    visit.join(page).foreach(println)
//
//    page.join(visit).foreach(println)

    val func = rectangle(4)
    println(func(5))


  }
}
