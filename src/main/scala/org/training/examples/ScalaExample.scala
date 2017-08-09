package org.training.examples

/**
  * Created by Administrator on 2017/7/10.
  */

object ScalaExample {
  //高阶函数测试
  def apply(f:Int => String,v:Int) = f(v)
  def layout[a](s:a) = "[" + s.toString() +"]"


  def main(args: Array[String]): Unit = {
    println(apply(layout,10))
  }
}
