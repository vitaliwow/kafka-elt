from dataclasses import dataclass

import duckdb

from topics import TOPICS
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

    def create_src_table(self, topic: str) -> str:
        handler = {
            "olist_customers_dataset": self.create_customer_table,
            "olist_order_items_dataset": self.create_order_items_table,
            "olist_orders_dataset": self.create_order_table,
            "product_category_name_translation": self.create_product_category_name_translation_table,
            "olist_sellers_dataset": self.create_sellers_table,
            "olist_geolocation_dataset": self.create_geolocation,
        }.get(topic)

        return str(handler().value)

    def create_facts_table(self) -> None:
        # create table
        query = FACTS_QUERIES[TableNames.FACTS_ORDER_ITEMS][TableOperations.CREATE]
        self.handle_query(query)

        #fill table
        insert_facts_table_query = FACTS_QUERIES[TableNames.FACTS_ORDER_ITEMS][TableOperations.INSERT]
        self.handle_query(insert_facts_table_query)

    def create_dimensions_tables(self) -> None:
        self.create_order_data_table()
        self.create_product_data_table()
        self.create_seller_data_table()
        # TODO OTHERS

    def create_staging_tables(self) -> None:
        self.create_staging_customer_deliveries()

    def create_staging_customer_deliveries(self) -> None:
        table = TableNames.STAGING_CUSTOMERS_DELIVERIES
        select_query = STAGING_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_analytical_tables(self) -> None:
        self.create_most_valuable_customers() # 1
        self.create_three_month_user_purchases() # 2
        self.create_between_orders_period() # 3
        self.create_top_ten_products() # 4
        self.create_raise_sales_gradient() # 5
        self.create_cumulative_product_sales() # 6
        self.create_daily_rebates() # 7
        self.create_avg_comparison() # 8
        self.create_sellers_rating() # 9
        self.create_trends_for_product_by_seller() # 10

    def create_order_data_table(self) -> None:
        # move to facts
        table = TableNames.DIMENSION_ORDER_DATA
        select = DIMENSION_QUERIES[table]

        self.create_table_from_select(select_query=select, table_name=table.value)

    def create_product_data_table(self) -> None:
        table = TableNames.DIMENSION_PRODUCT_DATA
        select = DIMENSION_QUERIES[table]

        self.create_table_from_select(select_query=select, table_name=table.value)

    def create_seller_data_table(self) -> None:
        table = TableNames.DIMENSION_SELLER_DATA
        select = DIMENSION_QUERIES[table]

        self.create_table_from_select(select_query=select, table_name=table.value)

    def create_most_valuable_customers(self) -> None:
        table = TableNames.ANALYTICS_MOST_VALUABLE_CUSTOMERS
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_three_month_user_purchases(self) -> None:
        table = TableNames.ANALYTICS_ROLLING_QUARTERS
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_between_orders_period(self) -> None:
        table = TableNames.ANALYTICS_ORDERS_AVG_PERIOD
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_trends_for_product_by_seller(self) -> None:
        table = TableNames.ANALYTICS_TREND_PRODUCTS_BY_SELLER
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_top_ten_products(self) -> None:
        # create top 10 products by sales in last quarter
        table = TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_SALES
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

        # create top 10 products by sales in last quarter by category
        table = TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_BY_CATEGORY
        select_query = ANALYTIC_QUERIES[table]
        self.create_table_from_select(select_query, table.value)

    def create_raise_sales_gradient(self) -> None:
        # create table with top 5 product categories by sales
        table = TableNames.ANALYTICS_RAISE_SALES_GRADIENT
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_cumulative_product_sales(self) -> None:
        # create cumulative product sales table
        table = TableNames.ANALYTICS_CUMULATIVE_PRODUCT_SALES
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_sellers_rating(self) -> None:
        table = TableNames.ANALYTICS_SELLER_RATING
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_daily_rebates(self) -> None:
        # create daily rebates table
        table = TableNames.ANALYTICS_DAILY_REBATES
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_table_from_select(self, select_query: str, table_name) -> None:
        result_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            {select_query}
        """
        self.handle_query(result_query)

    def create_avg_comparison(self) -> None:
        # create average comparison table
        table = TableNames.ANALYTICS_DAILY_AVGS_STATES_COMPARISON
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def insert_row_into_table(self, table_name: str, values: dict) -> None:
        data = values["data"]
        keys = ", ".join([k for k in data.keys()])

        formatted_values = []
        for x in data.values():
            if isinstance(x, str):
                x = x.replace("'", "?")
            formatted_values.append(f"'{x}'")

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
