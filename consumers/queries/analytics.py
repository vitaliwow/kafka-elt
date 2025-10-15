from enum_models import TableNames, OrderStatus


ANALYTIC_QUERIES = {
    TableNames.ANALYTICS_MOST_VALUABLE_CUSTOMERS: f"""
        SELECT 
            scd.customer_unique_id,
            SUM(scd.price) AS total_spent,
            COUNT(DISTINCT scd.order_id) AS total_orders,
            MAX(dord.order_delivered_customer_date) AS last_order_date,
            RANK() OVER (ORDER BY total_spent DESC) AS customer_rating
        FROM 
            {TableNames.FACTS_ORDER_ITEMS.value} scd
        LEFT JOIN 
            {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
            scd.order_id = dord.order_id
        WHERE dord.order_status = '{OrderStatus.DELIVERED.value}'
        GROUP BY 
        scd.customer_unique_id, 
        DATE_TRUNC('month', dord.order_delivered_customer_date)
        ORDER BY 
            customer_rating;
    """,
    TableNames.ANALYTICS_ROLLING_QUARTERS: f"""
        SELECT
            facts.customer_unique_id,
            DATE_TRUNC('month', dord.order_delivered_customer_date) AS month,
            SUM(price) AS monthly_total,
            AVG(monthly_total) OVER (
                PARTITION BY facts.customer_unique_id
                ORDER BY month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS rolling_quartal_avg
        FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
        LEFT JOIN 
            {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
            facts.order_id = dord.order_id
        GROUP BY    
            facts.customer_unique_id, 
            DATE_TRUNC('month', dord.order_delivered_customer_date)
        ORDER BY facts.customer_unique_id, month;
    """,
    TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_SALES: f"""
        WITH last_quarter AS (
            SELECT 
                DATE_TRUNC('quarter', MAX(order_purchase_timestamp)) AS quarter_start
            FROM {TableNames.DIMENSION_ORDER_DATA.value}
        ),
        quarterly_sales AS (
            SELECT 
                facts.product_id,
                SUM(facts.price) as total_quarter_sales
            FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
            CROSS JOIN last_quarter
            WHERE
                dord.order_purchase_timestamp >= last_quarter.quarter_start
                AND dord.order_purchase_timestamp < last_quarter.quarter_start + INTERVAL 1 QUARTER
            GROUP BY
                facts.product_id
        )
        SELECT 
            product_id,
            ROW_NUMBER() OVER (ORDER BY total_quarter_sales DESC) AS rank_by_quarter_sales,
            total_quarter_sales
        FROM quarterly_sales
        ORDER BY rank_by_quarter_sales
        LIMIT 10;
    """,
    TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_BY_CATEGORY: f"""
        WITH last_quarter AS (
            SELECT 
                DATE_TRUNC('quarter', MAX(order_purchase_timestamp)) AS quarter_start
            FROM {TableNames.DIMENSION_ORDER_DATA.value}
        ),
        category_product_sales AS (
            SELECT
                dpd.product_category_name_english,
                facts.product_id,
                SUM(facts.price) AS total_sales,
                ROW_NUMBER() OVER (
                    PARTITION BY dpd.product_category_name_english 
                    ORDER BY SUM(facts.price) DESC
                ) AS rank_by_category
            FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
            LEFT JOIN
                {TableNames.DIMENSION_PRODUCT_DATA.value} dpd ON
                facts.product_id = dpd.product_id
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
            CROSS JOIN last_quarter
            WHERE
                dord.order_purchase_timestamp >= last_quarter.quarter_start
                AND dord.order_purchase_timestamp < last_quarter.quarter_start + INTERVAL 1 QUARTER
            GROUP BY
                dpd.product_category_name_english,
                facts.product_id
        )
        SELECT
            product_category_name_english,
            product_id,
            total_sales,
            rank_by_category 
        FROM category_product_sales
        WHERE rank_by_category <= 10 AND product_category_name_english IS NOT NULL
        ORDER BY 
            product_category_name_english,
            rank_by_category;
    """,
    TableNames.ANALYTICS_RAISE_SALES_GRADIENT: f"""
    WITH top_5_categories AS (
        SELECT
            dpd.product_category_name_english as pcne,
            SUM(facts.price) as total_sales
        FROM {TableNames.FACTS_ORDER_ITEMS} facts
        LEFT JOIN
            {TableNames.DIMENSION_PRODUCT_DATA.value} dpd ON
            facts.product_id = dpd.product_id
        GROUP BY
            dpd.product_category_name_english
        ORDER BY total_sales DESC
        LIMIT 5
    ),
    monthly_sales AS (
        SELECT
            DATE_TRUNC('month', dord.order_purchase_timestamp) as month,
            dpd.product_category_name_english,
            SUM(facts.price) as monthly_sales
        FROM {TableNames.FACTS_ORDER_ITEMS} facts
        LEFT JOIN
            {TableNames.DIMENSION_PRODUCT_DATA.value} dpd ON
            facts.product_id = dpd.product_id
        LEFT JOIN 
            {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
            facts.order_id = dord.order_id
        WHERE dpd.product_category_name_english IN (SELECT pcne FROM top_5_categories)
        GROUP BY 
            DATE_TRUNC('month', dord.order_purchase_timestamp),
            dpd.product_category_name_english
    ),
    sales_with_growth AS (
        SELECT
            m.*,
            LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month) as prev_month_sales,
            monthly_sales - LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month) as sales_change,
            (monthly_sales - LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month)) / 
            NULLIF(LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month), 0) * 100 as growth_percentage
        FROM monthly_sales m
    )
    SELECT
        product_category_name_english as product_category,
        month,
        ROUND(growth_percentage, 2) as growth_percentage
    FROM sales_with_growth
    WHERE prev_month_sales IS NOT NULL
    ORDER BY
        product_category_name_english,
        month;
    """,
    TableNames.ANALYTICS_CUMULATIVE_PRODUCT_SALES: f"""
        WITH monthly_sales AS (
            SELECT 
                facts.product_id,
                EXTRACT(MONTH FROM dord.order_purchase_timestamp) AS month,
                SUM(facts.price) AS monthly_sales
            FROM {TableNames.FACTS_ORDER_ITEMS} facts
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
          WHERE EXTRACT(YEAR FROM dord.order_purchase_timestamp) = 2018
          GROUP BY product_id, EXTRACT(MONTH FROM dord.order_purchase_timestamp)
        )
        SELECT 
          product_id,
          SUM(CASE WHEN month <= 1 THEN monthly_sales ELSE 0 END) AS jan,
          SUM(CASE WHEN month <= 2 THEN monthly_sales ELSE 0 END) AS feb,
          SUM(CASE WHEN month <= 3 THEN monthly_sales ELSE 0 END) AS mar,
          SUM(CASE WHEN month <= 4 THEN monthly_sales ELSE 0 END) AS apr,
          SUM(CASE WHEN month <= 5 THEN monthly_sales ELSE 0 END) AS may,
          SUM(CASE WHEN month <= 6 THEN monthly_sales ELSE 0 END) AS jun,
          SUM(CASE WHEN month <= 7 THEN monthly_sales ELSE 0 END) AS jul,
          SUM(CASE WHEN month <= 8 THEN monthly_sales ELSE 0 END) AS aug,
          SUM(CASE WHEN month <= 9 THEN monthly_sales ELSE 0 END) AS sep,
          SUM(CASE WHEN month <= 10 THEN monthly_sales ELSE 0 END) AS oct,
          SUM(CASE WHEN month <= 11 THEN monthly_sales ELSE 0 END) AS nov,
          SUM(CASE WHEN month <= 12 THEN monthly_sales ELSE 0 END) AS dec,
        FROM monthly_sales
        GROUP BY product_id;
    """,
    TableNames.ANALYTICS_DAILY_REBATES: f"""
        WITH last_month_cutoff AS (
            SELECT MAX(order_purchase_timestamp) - INTERVAL 1 MONTH AS cutoff_date
            FROM {TableNames.DIMENSION_ORDER_DATA.value}
        ),
        last_month_order_purchases AS (
            SELECT 
                facts.order_id,
                dord.order_purchase_timestamp,
                SUM(facts.price) as order_price
            FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
            CROSS JOIN last_month_cutoff
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
            WHERE dord.order_purchase_timestamp >= last_month_cutoff.cutoff_date
            GROUP BY
                facts.order_id,
                dord.order_purchase_timestamp,
            ORDER BY dord.order_purchase_timestamp DESC
        )
        SELECT 
            DATE_TRUNC('day', last_month_order_purchases.order_purchase_timestamp) as day_of_month,
            ROUND(SUM(last_month_order_purchases.order_price), 2) as daily_rebates
        from last_month_order_purchases
        GROUP BY day_of_month;
    """,
    TableNames.ANALYTICS_DAILY_AVGS_STATES_COMPARISON: f"""
        WITH date_ranges AS (
            SELECT 
                DATE_TRUNC('month', MAX(dord.order_purchase_timestamp)) AS current_month_start,
                DATE_TRUNC('month', MAX(dord.order_purchase_timestamp)) - INTERVAL 1 MONTH AS prev_month_start,
                DATE_TRUNC('month', MAX(dord.order_purchase_timestamp)) - INTERVAL 2 MONTH AS two_months_ago_start
            FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
        ),
        current_month_stats AS (
            SELECT
                dsd.seller_state,
                AVG(facts.price) AS current_avg_order_value,
                COUNT(DISTINCT facts.order_id) AS current_order_count
            FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
            CROSS JOIN date_ranges
            LEFT JOIN 
                {TableNames.DIMENSION_SELLER_DATA.value} dsd ON 
                facts.seller_id = dsd.seller_id
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
            WHERE 
                dord.order_purchase_timestamp >= date_ranges.current_month_start
                AND dord.order_purchase_timestamp < date_ranges.current_month_start + INTERVAL 1 MONTH
            GROUP BY dsd.seller_state
        ),
        prev_month_stats AS (
            SELECT
                dsd.seller_state,
                AVG(facts.price) AS prev_avg_order_value,
                COUNT(DISTINCT facts.order_id) AS prev_order_count
            FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
            CROSS JOIN date_ranges
            LEFT JOIN 
                {TableNames.DIMENSION_SELLER_DATA.value} dsd ON 
                facts.seller_id = dsd.seller_id
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
            WHERE 
                dord.order_purchase_timestamp >= date_ranges.prev_month_start
                AND dord.order_purchase_timestamp < date_ranges.prev_month_start + INTERVAL 1 MONTH
            GROUP BY dsd.seller_state
        )
        SELECT
            DISTINCT dsd.seller_state AS state,
            COALESCE(c.current_avg_order_value, 0) AS current_avg_order_value,
            COALESCE(p.prev_avg_order_value, 0) AS prev_avg_order_value,
            ROUND((COALESCE(c.current_avg_order_value, 0) - p.prev_avg_order_value) / p.prev_avg_order_value * 100, 2) AS percentage_change,
        FROM {TableNames.DIMENSION_SELLER_DATA.value} dsd
        LEFT JOIN current_month_stats c ON dsd.seller_state = c.seller_state
        LEFT JOIN prev_month_stats p ON dsd.seller_state = p.seller_state
    """,
    TableNames.ANALYTICS_SELLER_RATING: f"""
        WITH date_range AS (
            SELECT 
                MAX(dord.order_purchase_timestamp) - INTERVAL 1 YEAR AS start_date,
                MAX(dord.order_purchase_timestamp) AS end_date
            FROM {TableNames.DIMENSION_ORDER_DATA.value} dord
        ),
        seller_metrics AS (
            SELECT
                facts.seller_id,
                SUM(facts.price) AS total_revenue,
                AVG(facts.price) AS avg_rating
            FROM {TableNames.FACTS_ORDER_ITEMS.value} facts
            CROSS JOIN date_range
            LEFT JOIN 
                {TableNames.DIMENSION_ORDER_DATA.value} dord ON 
                facts.order_id = dord.order_id
            WHERE 
                dord.order_purchase_timestamp BETWEEN date_range.start_date AND date_range.end_date
            GROUP BY facts.seller_id
        ),
        seller_scores AS (
            SELECT
                seller_id,
                PERCENT_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_percentile,
                PERCENT_RANK() OVER (ORDER BY avg_rating DESC) AS avg_rating
            FROM 
                seller_metrics
        )
        SELECT
            seller_id,
            ROW_NUMBER() OVER (
                ORDER BY (0.5 * revenue_percentile + 0.5 * avg_rating) DESC
            ) AS performance_rank
        FROM 
            seller_scores
        ORDER BY 
            performance_rank;
    """,
    TableNames.ANALYTICS_ORDERS_AVG_PERIOD: f"""
        WITH unique_orders AS (
            SELECT DISTINCT 
                customer_unique_id,
                order_id
            FROM {TableNames.FACTS_ORDER_ITEMS.value} 
        ),
        customer_orders AS (
        SELECT
            uo.customer_unique_id,
            uo.order_id,
            dord.order_purchase_timestamp order_timestamp,
            LEAD(order_timestamp) OVER (PARTITION BY uo.customer_unique_id ORDER BY order_timestamp) AS next_order_timestamp
        FROM
            unique_orders uo
        LEFT JOIN
            {TableNames.DIMENSION_ORDER_DATA.value} dord ON
            uo.order_id = dord.order_id
        )
        SELECT
            customer_unique_id,
            AVG(next_order_timestamp - order_timestamp) as interval
        FROM customer_orders
        WHERE next_order_timestamp IS NOT NULL
        GROUP BY 
            customer_unique_id,
        ORDER BY interval DESC
    """,
    TableNames.ANALYTICS_TREND_PRODUCTS_BY_SELLER: f"""
        SELECT 
            DATE_TRUNC('month', dord.order_purchase_timestamp) AS month,
            facts.seller_id,
            facts.product_id,
            COUNT(DISTINCT facts.order_id) AS orders_count,
        FROM {TableNames.FACTS_ORDER_ITEMS} facts
        JOIN {TableNames.DIMENSION_ORDER_DATA} dord
            ON dord.order_id = facts.order_id 
        GROUP BY month, facts.seller_id, facts.product_id
        ORDER BY facts.seller_id, facts.product_id, month
    """,
}
# тренд - относительно предыдущего месяца