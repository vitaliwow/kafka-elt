from dataclasses import dataclass

import duckdb

from topics import SOURCE_TOPICS
from .enum_models import TableNames, TableOperations
from .queries import (
    ANALYTIC_QUERIES,
    DIMENSION_QUERIES,
    FACTS_QUERIES,
    SOURCE_QUERIES,
    STAGING_QUERIES,
)


@dataclass
class HandleOlist:
    connection: duckdb.DuckDBPyConnection

    def create_facts_table(self) -> str:
        # create table
        table_name = TableNames.FACTS_ORDER_ITEMS
        query = FACTS_QUERIES[table_name.value][TableOperations.CREATE]
        self.handle_query(query)

        return table_name

    def insert_row_into_table(self, table_name: str, data: dict) -> None:
        keys = ", ".join([k for k in data.keys()])
        formatted_values = []
        for x in data.values():
            is_null = False

            if isinstance(x, str):
                x = x.replace("'", "?")
            if x is None:
                is_null = True
                x = "NULL"

            formatted_values.append(f"'{x}'" if not is_null else x)

        values = ", ".join(formatted_values)

        insert_query = f"""
            INSERT INTO {table_name}({keys}) VALUES ({values})
            ON CONFLICT DO NOTHING
        """
        try:
            self.handle_query(insert_query)
        except Exception as err:
            print(f"Data error {err}")
            pass

    def handle_query(self, query: str) -> None:
        self.connection.sql(query)

    def create_customer_table(self) -> TableNames:
        table = TableNames.SRC_CUSTOMERS
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]
        self.handle_query(sql_create)

        self.handle_query(sql_create)

        return table

    def create_order_items_table(self) -> None:
        table_name = TableNames.SRC_ORDER_ITEMS
        sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name.value} (
            order_id VARCHAR(100),
            order_item_id INT,
            product_id VARCHAR(100),
            seller_id VARCHAR(100),
            shipping_limit_date TIMESTAMP,
            price FLOAT,
            freight_value FLOAT,
            UNIQUE (order_id, order_item_id)
        )
        """

        self.handle_query(sql_create)

        return table_name

    def create_order_table(self) -> None:
        table_name = TableNames.SRC_ORDERS
        sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name.value} (
                order_id VARCHAR(100) PRIMARY KEY,
                customer_id VARCHAR(100),
                order_status VARCHAR(50),
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
            )
        """
        self.handle_query(sql_create)

        return table_name.value

    def create_products_table(self) -> None:
        table = TableNames.SRC_PRODUCTS
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]

        self.handle_query(sql_create)

    def create_product_category_name_translation_table(self) -> None:
        table = TableNames.SRC_PRODUCTS_CATEGORIES_TRANSLATIONS
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]

        self.handle_query(sql_create)

        return table.value

    def create_sellers_table(self) -> None:
        table = TableNames.SRC_SELLERS
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]

        self.handle_query(sql_create)

        return table.value

    def create_geolocation(self)  -> None:
        table = TableNames.SRC_GEO
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]

        self.handle_query(sql_create)
