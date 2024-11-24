import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataCleaning {

  def cleanCustomers(customers: DataFrame): DataFrame = {
    customers.filter(
      col("customer_id").isNotNull &&
        col("customer_unique_id").isNotNull &&
        col("customer_zip_code_prefix") > 0
    )
  }

  def cleanOrders(orders: DataFrame): DataFrame = {
    orders.filter(
      col("order_id").isNotNull &&
        col("customer_id").isNotNull
    ).dropDuplicates("order_id")
  }

  def cleanProducts(products: DataFrame): DataFrame = {
    products.withColumn("product_name_length", coalesce(col("product_name_length"), lit(0)))
      .withColumn("product_description_length", coalesce(col("product_description_length"), lit(0)))
      .withColumn("product_photos_qty", coalesce(col("product_photos_qty"), lit(0)))
      .withColumn("product_weight_g", coalesce(col("product_weight_g"), lit(0.0)))
      .withColumn("product_length_cm", coalesce(col("product_length_cm"), lit(0.0)))
      .withColumn("product_height_cm", coalesce(col("product_height_cm"), lit(0.0)))
      .withColumn("product_width_cm", coalesce(col("product_width_cm"), lit(0.0)))
  }

  def cleanItems(items: DataFrame): DataFrame = {
    items.filter(
      col("order_id").isNotNull &&
        col("product_id").isNotNull &&
        col("price") > 0
    ).dropDuplicates("order_id", "order_item_id")
  }
}
