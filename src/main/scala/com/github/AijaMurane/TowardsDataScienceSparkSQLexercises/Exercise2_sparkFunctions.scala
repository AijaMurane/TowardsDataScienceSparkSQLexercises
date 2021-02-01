/* Exercises: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* */

package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, broadcast}

object Exercise2_sparkFunctions {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /* For each seller, what is the average % contribution of an order to the seller's daily quota?
  # Example
  If Seller_0 with `quota=250` has 3 orders:
  Order 1: 10 products sold
  Order 2: 8 products sold
  Order 3: 7 products sold

  The average % contribution of orders to the seller's quota would be:
  Order 1: 10/105 = 0.04
  Order 2: 8/105 = 0.032
  Order 3: 7/105 = 0.028

  Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333 */

  val t0KS = System.nanoTime()

  val salesDF = spark.read.format("parquet").load("./src/resources/sales_parquet")
  val sellersDF = spark.read.format("parquet").load("./src/resources/sellers_parquet")

  salesDF
    .join(broadcast(sellersDF), salesDF("seller_id") <=> sellersDF("sellers_id"), "inner")
    .withColumn("ratio", salesDF("num_pieces_sold")/sellersDF("daily_target"))
    .groupBy(salesDF("seller_id"))
    .agg(avg("ratio"))
    .show()

  val t1t0KS = System.nanoTime()
  println("Elapsed time: " + (t1t0KS - t0KS) / 10e8 + "s")
}
