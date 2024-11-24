import org.apache.spark.sql.SparkSession
import DataLoading._
import DataCleaning._
import RecurrentClientAnalysis._
import analysis.{SalesAnalysis, CustomerSegmentation}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("E-commerce Data Analysis")
      .master("local[*]")
      .getOrCreate()

    try {
      println("----- E-commerce Data Analysis Workflow Started -----")

      // Step 1: Load Raw Data
      println("\n[Step 1: Loading Raw Data] Beginning...")
      val rawCustomers = loadCustomers(spark).toDF()
      val rawOrders = loadOrders(spark).toDF()
      val rawProducts = loadProducts(spark).toDF()
      val rawItems = loadItems(spark).toDF()
      println(s"[Step 1: Loading Raw Data] Completed. Dataset Sizes:")
      println(s"Raw Customers: ${rawCustomers.count()} rows")
      println(s"Raw Orders: ${rawOrders.count()} rows")
      println(s"Raw Products: ${rawProducts.count()} rows")
      println(s"Raw Items: ${rawItems.count()} rows")

      // Step 2: Clean Data
      println("\n[Step 2: Cleaning Data] Beginning...")
      val cleanedCustomers = cleanCustomers(rawCustomers)
      val cleanedOrders = cleanOrders(rawOrders)
      val cleanedProducts = cleanProducts(rawProducts)
      val cleanedItems = cleanItems(rawItems)
      println("[Step 2: Cleaning Data] Completed. Dataset Sizes:")
      println(s"Cleaned Customers: ${cleanedCustomers.count()} rows")
      println(s"Cleaned Orders: ${cleanedOrders.count()} rows")
      println(s"Cleaned Products: ${cleanedProducts.count()} rows")
      println(s"Cleaned Items: ${cleanedItems.count()} rows")

      // Step 3: Create Centralized View with Progressive Joins
      println("\n[Step 3: Creating Centralized View with Progressive Joins] Beginning...")
      val centralizedView = createCentralizedView(
        cleanedOrders,
        cleanedItems,
        cleanedProducts,
        cleanedCustomers
      )
      println(s"[Step 3: Centralized View Created] Dataset Size: ${centralizedView.count()} rows")
      centralizedView.show(10, truncate = false)

      // Step 4: Identify Recurrent Clients
      println("\n[Step 4: Identifying Recurrent Clients] Beginning...")
      val recurrentClients = findRecurrentClients(centralizedView)
      println(s"[Step 4: Recurrent Clients Identified] Count: ${recurrentClients.count()} rows")
      recurrentClients.show(10, truncate = false)

      // Step 5: Perform Sales Analysis
      println("\n[Step 5: Performing Sales Analysis] Beginning...")

      // (1) Sales by Customer
      val salesByCustomer = SalesAnalysis.salesByCustomer(cleanedItems, cleanedOrders, cleanedCustomers)
      println(s"\nSales by Customer: ${salesByCustomer.count()} rows")
      salesByCustomer.show(10, truncate = false)

      // (2) Average Basket Value by Category
      val avgBasketByCategory = SalesAnalysis.averageBasketByCategory(cleanedProducts, cleanedItems)
      println(s"\nAverage Basket Value by Category: ${avgBasketByCategory.count()} rows")
      avgBasketByCategory.show(10, truncate = false)

      // (3) Popular Products
      val popularProducts = SalesAnalysis.popularProducts(cleanedItems)
        .join(cleanedProducts, "product_id") // Join with products to include category names
      println("\nTop 10 Popular Products with Category Names:")
      popularProducts.select("product_id", "product_category_name", "order_count")
        .show(10, truncate = false)

      // (4) Popular Products for Recurrent Clients with Category Names
      val popularProductsForRecurrentClients =
        SalesAnalysis.popularProductsForRecurrentClients(recurrentClients, centralizedView)
          .join(cleanedProducts, "product_id") // Join to get category names
      println(s"\nPopular Products for Recurrent Clients with Category Names:")
      popularProductsForRecurrentClients.select("product_id", "product_category_name", "order_count")
        .show(10, truncate = false)

      println("[Step 5: Performing Sales Analysis] Completed.")

      // Step 6: Client Segmentation
      println("\n[Step 6: Performing Client Segmentation] Beginning...")

      // Segment customers based on the centralized view
      val customerSegments = CustomerSegmentation.segmentCustomers(centralizedView) // Ensure this returns DataFrame

      // Display the count of each segment (e.g., how many VIP, Loyal, Occasional)
      val segmentCounts = customerSegments
        .groupBy("segment")
        .agg(org.apache.spark.sql.functions.count("customer_id").alias("count")) // Count customers in each segment

      println("Segment Counts:")
      segmentCounts.show() // Display the count of each segment

      println("Sample of Client Segmentation:")
      customerSegments.show(10, truncate = false) // Display sample customer segments

      println("[Step 6: Performing Client Segmentation] Completed.")




      println("\n----- E-commerce Data Analysis Workflow Completed Successfully -----")

    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("Spark session stopped.")
    }
  }
}
