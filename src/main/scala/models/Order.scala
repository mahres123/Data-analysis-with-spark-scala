package models
case class Order(
                  order_id: String,
                  customer_id: String,
                  order_status: String,
                  order_purchase_timestamp: String, // Simplified to String
                  order_approved_at: String,        // Simplified to String
                  order_delivered_carrier_date: String, // Simplified to String
                  order_delivered_customer_date: String, // Simplified to String
                  order_estimated_delivery_date: String
                )
