package com.shan

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object q3Solu {
  def main(args: Array[String]): Unit = {
    //    if (args.length < 1) {
    //      System.err.print("Usage: com.shan.q2Solu <logfile>")
    //      System.exit(1)
    //    }


    val spark = SparkSession
      .builder()
      .appName("q3Solu")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
        import spark.implicits._

    val zcodeDF = spark.read.json("/loudacre/problem3/input")
    val zcodeCA = zcodeDF.where("state = 'CA'").select("zip","city")
    zcodeCA.write.parquet("loudacre/problem3/solution")
    zcodeCA.write.saveAsTable("problem3.solution")

  }
}