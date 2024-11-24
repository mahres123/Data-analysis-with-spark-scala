import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RecurrentClientAnalysis {

  /**
   * Create a centralized view with progressive joins.
   *
   * @param orders    Dataset of orders.
   * @param items     Dataset of items.
   * @param products  Dataset of products.
   * @param customers Dataset of customers.
   * @return Centralized DataFrame with combined data.
   */
  def createCentralizedView(
                             orders: DataFrame,
                             items: DataFrame,
                             products: DataFrame,
                             customers: DataFrame
                           ): DataFrame = {
    val ordersWithItems = orders.join(items, Seq("order_id"), "inner")
    val ordersItemsWithProducts = ordersWithItems.join(products, Seq("product_id"), "inner")
    val centralizedView = ordersItemsWithProducts.join(customers, Seq("customer_id"), "inner")
    centralizedView
  }

  /**
   * Identify recurrent clients.
   *
   * @param centralizedView Centralized DataFrame with combined data.
   * @return DataFrame of recurrent clients.
   */
  def findRecurrentClients(centralizedView: DataFrame): DataFrame = {
    import centralizedView.sparkSession.implicits._

    centralizedView
      .groupBy("customer_id") // Group by customer_id
      .count()               // Count the number of rows
      .filter($"count" > 1)  // Keep only customers with more than one order
  }

  /**
   * Identify popular products bought by recurrent clients.
   *
   * @param recurrentClients DataFrame of recurrent clients.
   * @param centralizedView  Centralized DataFrame with combined data.
   * @return DataFrame of popular products bought by recurrent clients.
   */
  def popularProductsForRecurrentClients (
                                              recurrentClients: DataFrame,
                                              centralizedView: DataFrame
                                            ): DataFrame = {


    // Join recurrent clients with the centralized view to get their transactions
    val recurrentClientTransactions = centralizedView
      .join(recurrentClients, Seq("customer_id"), "inner")

    // Group by product_id to find the most popular products
    recurrentClientTransactions
      .groupBy("product_id") // Group by product_id
      .agg(count("order_id").alias("order_count")) // Count the number of orders per product
      .orderBy(desc("order_count")) // Order by popularity
  }
}
