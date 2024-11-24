package sessionization

case class BehaviorSchema(
    event_time: String,
    event_type: String,
    product_id: Long,
    category_id: Long,
    category_code: String,
    brand: String,
    price: Double,
    user_id: Long,
    event_date: String,
    event_hour: String
)
