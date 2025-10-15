from enum_models import TableNames, TableOperations


SOURCE_QUERIES = {
    TableNames.SRC_CUSTOMERS: {
        TableOperations.CREATE: f"""
            CREATE TABLE IF NOT EXISTS {TableNames.SRC_CUSTOMERS.value} (
                customer_id VARCHAR(100) UNIQUE,
                customer_unique_id VARCHAR(100),
                customer_zip_code_prefix VARCHAR(10),
                customer_city VARCHAR(100),
                customer_state VARCHAR(5)
            )
        """,
    },
    TableNames.SRC_PRODUCTS: {
        TableOperations.CREATE: f"""
            CREATE TABLE IF NOT EXISTS {TableNames.SRC_PRODUCTS.value} (
                product_id VARCHAR(100) PRIMARY KEY,
                product_category_name VARCHAR(100),
                product_name_lenght INT,
                product_description_lenght INT,
                product_photos_qty INT,
                product_weight_g FLOAT,
                product_length_cm FLOAT,
                product_height_cm FLOAT,
                product_width_cm FLOAT
            )
        """
    },
    TableNames.SRC_PRODUCTS_CATEGORIES_TRANSLATIONS: {
        TableOperations.CREATE: f"""
            CREATE TABLE IF NOT EXISTS {TableNames.SRC_PRODUCTS_CATEGORIES_TRANSLATIONS.value} (
                product_category_name VARCHAR(100) UNIQUE,
                product_category_name_english VARCHAR(100)
            )
        """
    },
    TableNames.SRC_SELLERS: {
        TableOperations.CREATE: f"""
            CREATE TABLE IF NOT EXISTS {TableNames.SRC_SELLERS.value} (
                seller_id VARCHAR(100) PRIMARY KEY,
                seller_zip_code_prefix VARCHAR(10),
                seller_city VARCHAR(100),
                seller_state VARCHAR(5)
            )
        """
    },
    TableNames.SRC_GEO: {
        TableOperations.CREATE: f"""
        CREATE TABLE IF NOT EXISTS {TableNames.SRC_GEO.value} (
            geolocation_zip_code_prefix VARCHAR PRIMARY KEY,
            geolocation_lat FLOAT,
            geolocation_lng FLOAT,
            geolocation_city VARCHAR,
            geolocation_state VARCHAR(5),
            UNIQUE (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
        )
    """
    },
}
