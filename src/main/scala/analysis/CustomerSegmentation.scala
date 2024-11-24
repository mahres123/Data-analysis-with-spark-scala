package analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CustomerSegmentation {

  /**
   * Segment customers based on the count of their transactions.
   *
   * @param centralizedView DataFrame containing all customer transactions.
   * @return DataFrame containing customer_id, segment, and the count of each segment.
   */
  def segmentCustomers(centralizedView: DataFrame): DataFrame = {
    // Aggregate by customer_id and count transactions
    val customerTransactionCounts = centralizedView
      .groupBy("customer_id")
      .agg(count("*").alias("transaction_count")) // Count transactions per customer

    // Add segments based on transaction_count
    customerTransactionCounts.withColumn(
      "segment",
      when(col("transaction_count") > 4, "VIP") // More than 5 transactions = VIP
        .when(col("transaction_count") > 2, "Loyal") // More than 2 transactions = Loyal
        .otherwise("Occasional") // Otherwise = Occasional
    )
  }
}
