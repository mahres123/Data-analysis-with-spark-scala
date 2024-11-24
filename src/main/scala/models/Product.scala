package models

case class Product(product_id: String,
                   product_category_name: String,
                   product_name_length: Option[Int],          // Nullable field
                   product_description_length: Option[Int],  // Nullable field
                   product_photos_qty: Option[Int],          // Nullable field
                   product_weight_g: Option[Double],         // Nullable field
                   product_length_cm: Option[Double],        // Nullable field
                   product_height_cm: Option[Double],        // Nullable field
                   product_width_cm: Option[Double]          // Nullable field
                  )
