/* Exercises: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* Results are the same as in solutions.
* Key salting is suggested: https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c
* Another explanation about key salting: https://www.youtube.com/watch?v=d41_X78ojCg
* Key salting code: https://github.com/gjeevanm/SparkDataSkewness/blob/master/src/main/scala/com/gjeevan/DataSkew/RemoveDataSkew.scala
* */


package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, avg, col, concat, explode, floor, lit, monotonically_increasing_id, rand, agg}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Exercise1 extends App {
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
    .selectExpr("order_id", "product_id", "cast(num_pieces_sold as integer)")

  /*
    salesDF.createOrReplaceTempView("sales_view")
    productsDF.createOrReplaceTempView("products_view")

    val t0 = System.nanoTime()

    val revenueDF = spark.sql("SELECT sales_view.order_id, (sales_view.num_pieces_sold * products_view.price) AS revenue " +
      "FROM sales_view " +
      "JOIN products_view " +
      "WHERE sales_view.product_id = products_view.product_id")

    revenueDF.select(avg("revenue")).show()

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 10e8 + "s")
  */

  /* Using the "key salting" way. */

  val t0KS = System.nanoTime()

  /*
  def saltedJoin(df: DataFrame, buildDF: DataFrame, joinExpression: Column, joinType: String, salt: Int): DataFrame = {
    val tmpDF = buildDF
      .withColumn("slt_range", array(Range(0, salt).toList.map(lit): _*))
    val tableDF = tmpDF
      .withColumn("slt_ratio_s", explode(tmpDF("slt_range"))).drop("slt_range")
    val streamDF = df
      .withColumn("slt_ratio", monotonically_increasing_id % salt)
    val saltedExpr = streamDF("slt_ratio") === tableDF("slt_ratio_s") && joinExpression

    streamDF
      .join(tableDF, saltedExpr, joinType)
      .drop("slt_ratio_s")
      .drop("slt_ratio")
  }

  val joinedDF = productsDF
    .saltedJoin(salesDF, "left", 100)
    .show()*/

  /*
  val salt = 100

  val tmpDF = productsDF
    .withColumn("slt_range", array(Range(0, salt).toList.map(lit): _*))
  val tableDF = tmpDF
    .withColumn("slt_ratio_s", explode(tmpDF("slt_range"))).drop("slt_range")
  val streamDF = salesDF
    .withColumn("slt_ratio", monotonically_increasing_id % salt)
  //val joinExpression = "left"
  //val saltedExpr = streamDF("slt_ratio") === tableDF("slt_ratio_s") && joinExpression

  println("Salted table:")
  streamDF
    .join(tableDF, streamDF("slt_ratio") === tableDF("slt_ratio_s"), "left")
    .drop("slt_ratio_s")
    .drop("slt_ratio")
    .show()*/

  /*
  val results = salesDF
    .groupBy("product_id")
    .count()
    .orderBy(col("count").desc)
    .limit(100)

  results.show()

  //this array will consist only of the top 100 product
  val replicated_products
*/
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



  def elimnateDataSkew(leftTable: DataFrame, leftCol: String, rightTable: DataFrame) = {

    val df1 = leftTable
      .where(col("product_id") === "0")
      .withColumn(leftCol, concat(
        leftTable.col(leftCol), lit("_"), lit(floor(rand(123456) * 10))))
    val df2 = rightTable
      .where(col("product_id") === "0")
      .withColumn("explodedCol",
        explode(
          array((0 to 10).map(lit(_)): _ *)
        ))
      .withColumnRenamed("product_id", "product_id_drop")

    (df1, df2)
  }

  val (df3, df4) = elimnateDataSkew(salesDF, "product_id", productsDF)

  df3.printSchema()
  df3.show(5,false)

  df4.printSchema()
  df4.show(5,false)

  val joinedDF = df3
    .join(df4, df3.col("product_id")<=> concat(df4.col("product_id_drop"),lit("_"),df4.col("explodedCol")))
    .drop("product_id_drop", "explodeCol")

  joinedDF.printSchema()
  joinedDF.show(10,false)


  val revenueDF = joinedDF
    .selectExpr("order_id", "product_id", "price", "num_pieces_sold", "price*num_pieces_sold AS revenue")
    .show(10,false)

  val avgRevenue = revenueDF.agg(avg(col("revenue")))


  val t1t0KS = System.nanoTime()
  println("Elapsed time: " + (t1t0KS - t0KS) / 10e8 + "s")

}
