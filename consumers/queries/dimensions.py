from enum_models import TableNames


DIMENSION_QUERIES = {
    TableNames.DIMENSION_ORDER_DATA: f"""
            SELECT
                order_id,
                order_delivered_customer_date,  
                order_status,
                order_purchase_timestamp,
            FROM {TableNames.SRC_ORDERS.value};
            """,
    TableNames.DIMENSION_PRODUCT_DATA: f"""
        SELECT
            products.product_id,
            product_category_name_english
        FROM {TableNames.SRC_PRODUCTS.value} products
        LEFT JOIN {TableNames.SRC_PRODUCTS_CATEGORIES_TRANSLATIONS.value} categories ON
            products.product_category_name = categories.product_category_name;
    """,
    TableNames.DIMENSION_SELLER_DATA: f"""
        SELECT
            seller.seller_id,
            seller.seller_state
        FROM {TableNames.SRC_SELLERS.value} seller
    """,
}
