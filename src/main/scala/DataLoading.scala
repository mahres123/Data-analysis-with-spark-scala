import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataLoading {

  def loadCustomers(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/Amina/Desktop/IMSD/Scala/customers.csv")
      .select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix").cast("int"),
        col("customer_city"),
        col("customer_state")
      )
  }

  def loadOrders(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "false") // Avoid automatic type inference
      .csv("C:/Users/Amina/Desktop/IMSD/Scala/orders.csv")
      .select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date")
      )
  }

  def loadProducts(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/Amina/Desktop/IMSD/Scala/products.csv")
      .select(
        col("product_id"),
        col("product_category_name"),
        col("product_name_length").cast("int"),
        col("product_description_length").cast("int"),
        col("product_photos_qty").cast("int"),
        col("product_weight_g").cast("double"),
        col("product_length_cm").cast("double"),
        col("product_height_cm").cast("double"),
        col("product_width_cm").cast("double")
      )
      .na.fill(0) // Replace null values with 0 for numeric columns
  }

  def loadItems(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/Amina/Desktop/IMSD/Scala/items.csv")
      .select(
        col("order_id"),
        col("order_item_id").cast("int"),
        col("product_id"),
        col("seller_id"),
        col("shipping_limit_date"),
        col("price").cast("double"),
        col("freight_value").cast("double")
      )
  }
}
