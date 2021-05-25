package com.shan

import org.apache.spark.sql.hive.
import org.apache.spark.{SparkConf, SparkContext}

object q3Solu {
  def main(args: Array[String]): Unit = {
    //    if (args.length < 1) {
    //      System.err.print("Usage: com.shan.q2Solu <logfile>")
    //      System.exit(1)
    //    }

    //TODO: complete exercise
    //1. 构建SparkConf对象，设置application的名称
    val sparkConf = new SparkConf().setAppName("q3Solu").setMaster("local[2]")
    //2. 构建SparkContext对象，该对象非常重要，它是所有spark程序的执行入口
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val hivec = new HiveContext





  }
}