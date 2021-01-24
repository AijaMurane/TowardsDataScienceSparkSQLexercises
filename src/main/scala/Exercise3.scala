import org.apache.spark.sql.SparkSession

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

  salesDF
    .groupBy("product_id", "seller_id")
    .sum("num_pieces_sold")
    .show()


  /* Who are those for product with `product_id = 0` */

  val productsDF = spark.read.format("parquet").load("./src/resources/products_parquet")
}
