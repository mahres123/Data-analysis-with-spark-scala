package models



case class CustoOrders(
                        order_id: String,
                        customer_id: String,
                        customer_unique_id: String,
                        customer_zip_code_prefix: String,
                        customer_city: String,
                        customer_state: String,
                        order_status: String,
                        order_purchase_timestamp: String,
                        order_approved_at: String,
                        order_delivered_customer_date: String,
                        order_estimated_delivery_date: String
                      )
