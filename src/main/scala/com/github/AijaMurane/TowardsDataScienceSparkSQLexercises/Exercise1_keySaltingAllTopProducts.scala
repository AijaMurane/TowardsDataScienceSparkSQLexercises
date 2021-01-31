/* Exercises: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* Results are the same as in solutions.
* Key salting is suggested: https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c
* Here I rewrote the solution in Scala.
* */

package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, avg, broadcast, col, concat, explode, floor, lit, monotonically_increasing_id, rand, round, when}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType
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
    .map(_.replaceAll("\\[|]", ""))

  val REPLICATION_FACTOR = 101
  var l : List[(String,Int)] = List()
  for ( r <- 0 to 99) {
    for ( i <- 0 to 100) {
      l = l:+((replicated_products(r),i))
    }
  }

  import spark.implicits._

  val rdd = spark.sparkContext.parallelize(l)
  val columns = Seq("product_id","replication")
  val replicated_df = spark.createDataFrame(rdd).toDF(columns:_*)

  //productsDFnew is correct
  val productsDFnew = productsDF
    .join(broadcast(replicated_df), productsDF.col("product_id") <=> replicated_df.col("product_id"), "left")
    .withColumn("salted_join_key", when(replicated_df("replication").isNull, productsDF("product_id"))
      .otherwise(concat(replicated_df("product_id"), lit("-"), replicated_df("replication"))))

  val salesDFnew = salesDF
    .withColumn("salted_join_key", when(salesDF.col("product_id").isin(replicated_products:_*),
    concat(salesDF.col("product_id"), lit("-"),
      round(rand() * (REPLICATION_FACTOR - 1), 0).cast(
        IntegerType)))
      .otherwise(salesDF.col("product_id")))

  salesDFnew
    .join(productsDFnew, salesDFnew.col("salted_join_key") <=> productsDFnew.col("salted_join_key"),"inner")
    .agg(avg(productsDFnew("price") * salesDFnew("num_pieces_sold")))
    .show()

  val t1t0KS = System.nanoTime()
  println("Elapsed time: " + (t1t0KS - t0KS) / 10e8 + "s")
}
