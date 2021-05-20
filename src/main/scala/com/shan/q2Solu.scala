package com.shan

import org.apache.spark.{SparkContext, SparkConf}

object q2Solu {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.print("Usage: com.shan.q2Solu <logfile>")
      System.exit(1)
    }

    //TODO: complete exercise
    //1. 构建SparkConf对象，设置application的名称
    val sparkConf = new SparkConf().setAppName("acctPre_Online").setMaster("local[2]")
    //2. 构建SparkContext对象，该对象非常重要，它是所有spark程序的执行入口
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val acct = sc.textFile("/loudacre/accounts")
    val wlog = sc.textFile("loudacre/weblogs")
    val acct1 = acct.map(_.split(",")).map(ar => (ar(0), (ar(3), ar(4))))
    val wlog1 = wlog.map(_.split(" "))



  }
