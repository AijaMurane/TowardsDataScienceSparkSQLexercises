import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

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

  val productsDF = spark.read.format("parquet").load("./src/resources/products_parquet")
  val salesDF = spark.read.format("parquet").load("./src/resources/sales_parquet")

  salesDF.createOrReplaceTempView("sales_view")
  productsDF.createOrReplaceTempView("products_view")

  val revenueDF = spark.sql("SELECT sales_view.order_id, (sales_view.num_pieces_sold * products_view.price) AS revenue " +
    "FROM sales_view " +
    "JOIN products_view " +
    "WHERE sales_view.product_id = products_view.product_id")

  revenueDF.select(avg("revenue")).show()

}
