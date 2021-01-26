package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.SparkSession

object Exercise4 extends App {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val salesDF = spark.read.format("parquet").load("./src/resources/sales_parquet")


}
