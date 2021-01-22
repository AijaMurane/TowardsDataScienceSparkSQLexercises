//https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object WarmUp1 extends App {
  val spark = SparkSession.builder
  .master("local")
  .config("spark.sql.autoBroadcastJoinThreshold", -1)
  .config("spark.executor.memory", "500mb")
  .appName("Exercise1")
  .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val productsDF = spark.read.format("parquet").load("./src/resources/products_parquet")
  productsDF.printSchema()

  val salesDF = spark.read.format("parquet").load("./src/resources/sales_parquet")
  salesDF.printSchema()

  val sellersDF = spark.read.format("parquet").load("./src/resources/sellers_parquet")
  sellersDF.printSchema()

  /* Find out how many orders, how many products and how many sellers are in the data.*/

  val productsCount = productsDF.count()
  val salesCount = salesDF.count()
  val sellersCount = sellersDF.count()

  println(s"There are $salesCount orders, $productsCount products and $sellersCount sellers in the data.")

  /*How many products have been sold at least once?*/
  salesDF.createOrReplaceTempView("sales_view")
  val productsSoldOnceDF = spark.sql("SELECT DISTINCT product_id FROM sales_view")
  val productsSoldOnceCount = productsSoldOnceDF.count()

  println(s"$productsSoldOnceCount products have been sold at least once.")

  spark.sql("SELECT * FROM sales_view").show()

  /* Which is the product contained in more orders?*/
  productsDF.createOrReplaceTempView("products_view")
  spark.sql("SELECT * FROM products_view").show()


  val productsSoldMostDF = spark.sql("SELECT COUNT(sales_view.product_id) AS product_id_count, products_view.product_name " +
    "FROM sales_view " +
    "JOIN products_view " +
    "ON sales_view.product_id = products_view.product_id " +
    "ORDER BY sales_view.product_id " +
    "GROUP BY sales_view.product_id")

  productsSoldMostDF.show()


  //val mostSoldProduct = productsSoldMostDF.select(col("product_id")).first().toString().stripPrefix("[").stripSuffix("]")
  //println(s"Most sold product id was $mostSoldProduct")

}
