/* Exercises: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* Results are the same as in solutions.
* Key salting is suggested: https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c
* Here I rewrote the solution in Scala.
* */

package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, avg, col, concat, explode, floor, lit, monotonically_increasing_id, rand}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Exercise1_keySaltingAllTopProducts extends App {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /* What is the average revenue of the orders? */
  /* From exercise description: Each row in this table is an order and every order can contain only one product.*/


  val productsDF = spark
    .read
    .format("parquet")
    .load("./src/resources/products_parquet")
    .selectExpr("product_id", "cast(price as integer)")

  val salesDF = spark
    .read
    .format("parquet")
    .load("./src/resources/sales_parquet")
    .selectExpr("order_id", "cast(product_id as string)", "cast(num_pieces_sold as integer)")

  val t0KS = System.nanoTime()

  val results = salesDF
    .groupBy("product_id")
    .count()
    .orderBy(col("count").desc)
    .limit(100)

  val replicated_products = results
    .select("product_id")
    .collect
    .map(_.toString())

  val REPLICATION_FACTOR = 101
  var l = collection.mutable.Seq[(String, Int)]()
  for ( r <- 0 to 99) {
    for ( i <- 0 to 100)
      l :+ (replicated_products(r), i)
  }

  l.foreach(println)

  //val rdd = spark.sparkContext.parallelize()

  //val rdd = spark.sparkContext.parallelize(l)

  /*
  val REPLICATION_FACTOR = 101
  val l = ArrayBuffer[String]()
  val replicated_products = ArrayBuffer[String]()
  for (r <- 0 to results) replicated_products.append(_r["product_id"])
  for _rep in range(0, REPLICATION_FACTOR):
    l.append((_r["product_id"], _rep))
  rdd = spark.sparkContext.parallelize(l)
  replicated_df = rdd.map(lambda x: Row(product_id=x[0], replication=int(x[1])))
  replicated_df = spark.createDataFrame(replicated_df)
*/



  val t1t0KS = System.nanoTime()
  println("Elapsed time: " + (t1t0KS - t0KS) / 10e8 + "s")
}
