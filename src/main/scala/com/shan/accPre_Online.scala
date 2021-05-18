package com.shan

import org.apache.spark.{SparkConf, SparkContext}

//TODO:利用scala语言开发spark入门程序（提交到spark集群中运行）
object accPre_Online {

  def main(args: Array[String]): Unit = {
//    if (args.length < 1) {
//      System.err.print("Usage: com.shan.acctPre <logfile>")
//      System.exit(1)
//    }

    //TODO: complete exercise
    //1. 构建SparkConf对象，设置application的名称
    val sparkConf = new SparkConf().setAppName("acctPre_Online").setMaster("local[2]")
    //2. 构建SparkContext对象，该对象非常重要，它是所有spark程序的执行入口
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val acct = sc.textFile(args(0))
    val res = acct.map(_.split(",")).filter(ar => ar(7) == "CA").map(ar => (ar(7) + ":" + ar(6), (1, ar(3).length))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).map { case (city, (acnum, nlen)) => (city + "|" + acnum + "|" + nlen) }
    res.saveAsTextFile(args(1))

    sc.stop()
  }

}
