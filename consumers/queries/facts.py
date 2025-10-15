from enum_models import TableNames, TableOperations


FACTS_QUERIES = {
    TableNames.FACTS_ORDER_ITEMS: {
        TableOperations.CREATE: f"""
            CREATE SEQUENCE IF NOT EXISTS seq_facts_id START 1;
            
            CREATE TABLE IF NOT EXISTS {TableNames.FACTS_ORDER_ITEMS.value} (
                id INTEGER PRIMARY KEY DEFAULT nextval('seq_facts_id'),
                order_id VARCHAR(36),
                order_item_id INT,
                product_id VARCHAR(36),
                seller_id VARCHAR(36),
                customer_unique_id VARCHAR(36),
                price DECIMAL(10, 2)
            )
            """,
        TableOperations.INSERT: f"""
            INSERT INTO {TableNames.FACTS_ORDER_ITEMS.value}
            SELECT
                ord.order_id,
                ordi.order_item_id,
                ordi.product_id,
                ordi.seller_id,
                cust.customer_unique_id,
                ordi.price
            FROM {TableNames.SRC_ORDER_ITEMS.value} as ordi
            LEFT JOIN {TableNames.SRC_ORDERS.value} as ord
                ON ord.order_id = ordi.order_id
            LEFT JOIN {TableNames.SRC_CUSTOMERS.value} as cust
                ON ord.customer_id = cust.customer_id
            LEFT JOIN {TableNames.SRC_PRODUCTS.value} as prod
                ON ordi.product_id = prod.product_id
            """
    }
}
