from enum import StrEnum


class TableNames(StrEnum):
    # Source tables
    SRC_CUSTOMERS = "src_customers"
    SRC_SELLERS = "src_sellers"
    SRC_ORDER_ITEMS = "src_order_items"
    SRC_ORDERS = "src_orders"
    SRC_PRODUCTS = "src_products"
    SRC_GEO = "src_geolocation"
    SRC_PRODUCTS_CATEGORIES_TRANSLATIONS = "src_products_categories_translations"
    # Facts tables
    FACTS_ORDER_ITEMS = "facts_order_items"
    # Dimensions tables
    DIMENSION_ORDER_DATA = "dimension_order_data"
    DIMENSION_PRODUCT_DATA = "dimension_product_data"
    DIMENSION_SELLER_DATA = "dimension_seller_data"
    # Staging tables
    STAGING_CUSTOMERS_DELIVERIES = "staging_customers_deliveries"
    # Analytics tables
    ANALYTICS_MOST_VALUABLE_CUSTOMERS = "analytics_most_valuable_customers"
    ANALYTICS_ROLLING_QUARTERS = "analytics_rolling_quarters"
    ANALYTICS_TOP_PRODUCTS = "analytics_top_products"
    ANALYTICS_ORDERS_AVG_PERIOD = "analytics_orders_avg_period"
    ANALYTICS_TREND_PRODUCTS_BY_SELLER = "analytics_orders_avg_period"
    ANALYTICS_TOP_TEN_PRODUCTS_Q_SALES = "analytics_top_ten_products_q_sales"
    ANALYTICS_TOP_TEN_PRODUCTS_Q_BY_CATEGORY = "analytics_top_ten_products_q_by_category"
    ANALYTICS_RAISE_SALES_GRADIENT = "analytics_raise_sales_gradient"
    ANALYTICS_CUMULATIVE_PRODUCT_SALES = "analytics_cumulative_product_sales"
    ANALYTICS_DAILY_REBATES = "analytics_daily_rebates"
    ANALYTICS_DAILY_AVGS_STATES_COMPARISON = "analytics_daily_avgs_states_comparison"
    ANALYTICS_SELLER_RATING = "analytics_seller_rating"


class TableOperations(StrEnum):
    CREATE = "create"
    INSERT = "insert"


class OrderStatus(StrEnum):
    DELIVERED = "delivered"
    CANCELED = "canceled"
    PROCESSING = "processing"
