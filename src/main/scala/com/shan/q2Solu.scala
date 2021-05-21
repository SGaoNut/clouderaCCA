package com.shan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
//import com.databricks.spark.avro._

object q2Solu {
  def main(args: Array[String]): Unit = {
//    if (args.length < 1) {
//      System.err.print("Usage: com.shan.q2Solu <logfile>")
//      System.exit(1)
//    }

    //TODO: complete exercise
    //1. 构建SparkConf对象，设置application的名称
    val sparkConf = new SparkConf().setAppName("acctPre_Online").setMaster("local[2]")
    //2. 构建SparkContext对象，该对象非常重要，它是所有spark程序的执行入口
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val acct = sc.textFile("hdfs://localhost:8020/loudacre/accounts")
    val wlog = sc.textFile("hdfs://localhost:8020/loudacre/weblogs")
    val acct1 = acct.map(_.split(",")).map(ar => (ar(0), (ar(3), ar(4))))
    val wlog1 = wlog.map(_.split(" ")).map(ar => (ar(2), (ar(0))))
    val acct_wlog = acct1.join(wlog1).map{case (id,((fname, lname),ip_addr)) => (ip_addr,lname,fname)}
    val sparkSession = SparkSession.builder().appName("q2Solu").master("local").enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    val res = acct_wlog.toDF("ip_addr","lname","fname")
    res.write.orc("hdfs://localhost:8020/loudacre/problem2/solution/orc")
//    res.write.avro("hdfs://localhost:8020/loudacre/problem2/solution/avro")

  }
}
