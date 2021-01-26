package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

object Exercise3 extends App {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /* Who are the second most selling and the least selling persons (sellers) for each product? */

  val salesDF = spark
    .read
    .format("parquet")
    .load("./src/resources/sales_parquet")
    .selectExpr("product_id", "seller_id", "cast(num_pieces_sold as integer)")
  val sellersDF = spark.read.format("parquet").load("./src/resources/sellers_parquet")

  salesDF.printSchema()

  val salesAnalysisDF = salesDF
    .groupBy("product_id", "seller_id")
    .sum("num_pieces_sold").alias("sum_pieces_sold")
    .orderBy("product_id")

  val windowSpec = Window
    .partitionBy("product_id")
    .orderBy(col("sum(num_pieces_sold)").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  println("Second most selling sellers for each product are the following:")
  salesAnalysisDF
    .withColumn("rating", dense_rank() over windowSpec)
    .filter("rating=2")
    .orderBy("product_id")
    .show(5)

  println("The least selling sellers for each product are the following:")
  salesAnalysisDF
    .withColumn("rating", dense_rank() over windowSpec)
    .orderBy(col("rating").asc)
    .filter("rating=1")
    .orderBy("product_id")
    .show(5)

  /* Who are those for product with `product_id = 0` */

  println("Second most selling seller for product with 'product_id = 0' is the following seller:")
  salesAnalysisDF
    .withColumn("rating", dense_rank() over windowSpec)
    .filter("product_id=0")
    .filter("rating=2")
    .orderBy("product_id")
    .show(5)

  println("The least selling seller for product with 'product_id = 0' is the following seller:")
  salesAnalysisDF
    .withColumn("rating", dense_rank() over windowSpec)
    .filter("product_id=0")
    .orderBy(col("rating").asc)
    .filter("rating=1")
    .orderBy("product_id")
    .show(5)


}
