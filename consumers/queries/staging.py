from enum_models import TableNames


STAGING_QUERIES = {
    TableNames.STAGING_CUSTOMERS_DELIVERIES: f"""
    SELECT
        foi.customer_unique_id,
        foi.seller_id,
        foi.order_delivered_customer_date,
        foi.price,
        foi.order_id,
        foi.order_status,
        foi.product_id,
        foi.order_purchase_timestamp,
        foi.product_category_name_english,
        foi.seller_state,
    FROM
        {TableNames.FACTS_ORDER_ITEMS.value} foi
    """
}
