package com.shan

import org.apache.spark.{SparkConf, SparkContext}

object accPre {
  class acctPre {
    def main(args: Array[String]): Unit = {
      if (args.length < 1) {
        System.err.print("Usage: com.shan.acctPre <logfile>")
        System.exit(1)
      }

      //TODO: complete exercise
      val sconf = new SparkConf().setAppName("acctPre")
      val sc = new SparkContext(sconf)
      sc.setLogLevel("WARN")

      val acct = sc.textFile("/loudacre/accounts")
      val res = acct.map(_.split(",")).filter(ar => ar(7) == "CA").map(ar => (ar(7) + ":" + ar(6), (1, ar(3).length))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).map { case (city, (acnum, nlen)) => (city + "|" + acnum + "|" + nlen) }
      res.saveAsTextFile("/loudacre/problem1/solution")

      sc.stop()
    }
  }
}
