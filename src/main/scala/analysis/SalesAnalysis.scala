package analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SalesAnalysis {

  /**
   * Calculate total sales and order count by customer.
   *
   * @param items     DataFrame of items.
   * @param orders    DataFrame of orders.
   * @param customers DataFrame of customers.
   * @return DataFrame with customer_id, order_count, and total_spent.
   */
  def salesByCustomer(
                       items: DataFrame,
                       orders: DataFrame,
                       customers: DataFrame
                     ): DataFrame = {
    val itemsWithOrders = items.join(orders, Seq("order_id"), "inner")
    val itemsOrdersWithCustomers = itemsWithOrders.join(customers, Seq("customer_id"), "inner")

    itemsOrdersWithCustomers
      .groupBy("customer_id")
      .agg(
        countDistinct("order_id").alias("order_count"),
        sum("price").alias("total_spent")
      )
      .orderBy(desc("total_spent"))
  }

  /**
   * Calculate average basket value by product category.
   *
   * @param products DataFrame of products.
   * @param items    DataFrame of items.
   * @return DataFrame with product_category_name and average_basket_value.
   */
  def averageBasketByCategory(
                               products: DataFrame,
                               items: DataFrame
                             ): DataFrame = {
    val itemsWithProducts = items.join(products, Seq("product_id"), "inner")

    itemsWithProducts
      .groupBy("product_category_name")
      .agg(
        sum("price").alias("total_revenue"),
        count("order_id").alias("total_orders")
      )
      .withColumn("average_basket_value", col("total_revenue") / col("total_orders"))
      .select("product_category_name", "average_basket_value")
      .orderBy(desc("average_basket_value"))
  }

  /**
   * Identify the most popular products.
   *
   * @param items DataFrame of items.
   * @return DataFrame with product_id and order_count of the top 10 products.
   */
  def popularProducts(items: DataFrame): DataFrame = {
    items.groupBy("product_id")
      .agg(count("order_id").alias("order_count"))
      .orderBy(desc("order_count"))
      .limit(10)
  }

  /**
   * Popular products ordered by recurrent clients.
   *
   * @param recurrentClients DataFrame of recurrent client IDs.
   * @param centralizedView  Centralized DataFrame with combined data.
   * @return DataFrame containing product_id and order_count.
   */
  def popularProductsForRecurrentClients(
                                          recurrentClients: DataFrame,
                                          centralizedView: DataFrame
                                        ): DataFrame = {
    val recurrentClientTransactions = centralizedView.join(recurrentClients, Seq("customer_id"), "inner")

    recurrentClientTransactions
      .groupBy("product_id")
      .agg(count("order_id").alias("order_count"))
      .orderBy(desc("order_count"))
  }
}
