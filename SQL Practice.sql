--raw_contacts
--contact_id	full_name	email			phone_number
--001			John Doe	john@email.com	1234567890
--002			NULL						+60123456789
--003						NULL			NULL
--004			Jane Lee	jane@email.com	
--005			Alex		alex@x.com		(012) 345-6789
--006			NULL		NULL			+6011 888 9999

--?? Your Task:
--Write a query to:

--Clean full_name, replacing NULL or blanks with 'Unknown', and trimming whitespace
--Clean email, doing the same as above

--For phone_number:
--Replace NULL or blanks with 'No number'
--Also trim the number (but no need to standardize the format yet)



WITH cleaned_raw_contacts AS (
SELECT
	contact_id AS c_contact_id,
	CASE
	WHEN TRIM(full_name) IS NULL OR TRIM(full_name) = '' THEN 'Unknown'
	ELSE TRIM(full_name)
	END AS c_full_name,
	
	CASE
	WHEN TRIM(email) IS NULL OR TRIM(email) = '' THEN 'Unknown'
	ELSE TRIM(email)
	END AS c_email,
	-- phone number can be cleaned to remove ( ) + and if we need to have 0 as the first number, then case when substring(1,1) = 0 THEN keep phone_number, else concat 0 with phone_number. Best to do this after the cleaning
	CASE
	WHEN TRIM(phone_number) IS NULL OR TRIM(phone_number) = '' THEN 'No number'
	ELSE TRIM(phone_number)
	END AS c_phone_number,
FROM raw_contacts
)



--
--
--
--
--
--
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--?? Scenario:
--You work at an e-commerce company and want to generate a monthly customer report with these rules:

--Raw Tables:
--orders: order_id, customer_id, order_date, total_amount
--products: product_id, category, price
--order_items: order_id, product_id, quantity
--customers: customer_id, name, email, signup_date

--?? Desired Output:
--customer_id	month	total_orders	total_spent	repeat_buyer	favorite_category


-- CTE#1 clean primary order table - customer_id, month
WITH cleaned_orders AS (
SELECT
	TRIM(order_id) AS c_order_id,
	TRIM(customer_id) AS c_customer_id,
	TRY_CAST(order_date AS DATE) AS c_order_date,
	FORMAT(TRY_CAST(order_date AS DATE),'YYYY-MM') as c_month,
	TRY_CAST(total_amount AS DECIMAL(10,2)) AS c_total_amount
FROM orders
),

-- CTE#2 aggregate metrics for total orders and total spent by customer id and by month
agg_order AS (
SELECT
	co.c_customer_id AS agg_customer_id,
	co.c_month AS agg_month,
	COUNT(co.c_order_id) AS agg_total_orders,
	SUM(co.c_total_amount) AS agg_total_spent,
	CASE
	WHEN COUNT(co.c_order_id) > 1 THEN 'Repeat Buyer'
	ELSE 'Non Repeat Buyer'
	END AS agg_repeat_buyer,
FROM cleaned_orders AS co
GROUP BY co.c_customer_id, co.c_month
),

-- CTE#3 inner join primary order table to connect categories to the order table then rank category bucketed by customer by month, no bucketing for category
category_rank AS (
SELECT
	co.c_customer_id as cr_customer_id,
	co.c_month as cr_month,
	p.category as cr_category,
	ROW_NUMBER() OVER (PARTITION BY co.c_customer_id, co.c_month ORDER BY COUNT(co.c_order_id) DESC) AS cr_cat_rank
FROM clean_orders AS co
INNER JOIN order_items AS oi ON co.c_order_id = oi.order_id
INNER JOIN products AS p ON oi.product_id = p.product_id
GROUP BY co.c_customer_id, co.c_month, p.category -- we use COUNT order_id aggregate, so need to group
)

-- final query to join CTE#2 aggregates with #1 category for each customer_id by month. We should get total orders from all categories and total spent from all categories bucketed by customer and month
SELECT
	ao.agg_customer_id as customer_id,
	ao.agg_month as month,
	ao.agg_total_orders as total_orders,
	ao.agg_total_spent as total_spent,
	ao.agg_repeat_buyer as repeat_buyer,
	cr.cr_category as favourite_category
FROM agg_orders AS ao
LEFT JOIN category_rank AS cr
ON ao.agg_customer_id = cr.cr_customer_id AND ao.agg_month = cr.cr_month AND cr.cr_cat_rank = 1







--
--
--
--
--
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--?? Day 7: Mini Project – Customer Purchase Funnel
--?? Scenario:
--You're working as an analyst for a mid-sized e-commerce platform, and your manager wants an end-to-end funnel report for customer activity in Q1 2025.

--You’ve been given access to 4 raw tables:
--customers(customer_id, name, signup_date)
--orders(order_id, customer_id, order_date, total_amount)
--order_items(order_id, product_id, quantity)
--products(product_id, category, price)

--?? Goal: Customer Funnel Dashboard (Q1 2025)
--Generate a table that shows the funnel stages below:
--customer_id		signup_date		has_orders		total_orders		total_spent		first_order_date	favorite_category	is_repeat_buyer

--Logic per field:
--has_orders: Yes/No — Did they place at least one order in Q1 2025?
--total_orders: Count of orders placed in Q1
--total_spent: Sum of all order values in Q1
--first_order_date: Earliest order date in Q1
--favorite_category: Most frequently purchased category in Q1
--is_repeat_buyer: Yes/No — More than 1 order?

-- approach:
-- [cte to clean] all tables
-- [2025 orders] cte to filter cleaned orders table for 2025 only
-- [cte to aggregate] from filtered cleaned orders table, has_orders via count(order_id) CASE WHEN > 1 yes/no,
-- [cte to aggregate] total_orders via count(order_id), total_spent via sum(total_amount), first_order_date via min(order_date), is_repeat_buyer via count(order_id) CASE WHEN > 1 yes/no
-- [cte to aggregate] group by customer
-- [cte to rank for fav category] inner join 2025 order table with order items table and products table to connect category to 2025 order,
-- [cte to rank for fav category] row_number() over (partition by customer id order by count(order_id) DESC) as category rank
-- final query to take aggregate table and left join with fav category on customer id and category rank = 1 only

-- solution:
-- CTE#1 clean
WITH cleaned_customers AS (
SELECT
	TRIM(c.customer_id) as cleaned_customer_id,
	TRIM(c.name) AS cleaned_name,
	TRY_CAST(TRIM(c.signup_date) AS DATE) AS cleaned_signup_date
FROM customers AS c
),

-- CTE#2 clean
cleaned_orders AS (
SELECT
	TRIM(o.order_id) AS cleaned_order_id,
	TRIM(o.customer_id) AS cleaned_customer_id,
	TRY_CAST(TRIM(o.order_date) AS DATE) AS cleaned_order_date,
	TRY_CAST(TRIM(total_amount) AS DECIMAL(10,2)) AS cleaned_total_amount
FROM orders AS o
),

-- CTE#3 clean
cleaned_order_items AS (
SELECT
	TRIM(oi.order_id) AS cleaned_order_id,
	TRIM(oi.product_id) AS cleaned_product_id,
	TRY_CAST(TRIM(oi.quantity) AS INT) AS cleaned_quantity
FROM order_items AS oi
),

-- CTE#4 clean
cleaned_products AS (
SELECT
	TRIM(p.product_id) AS cleaned_product_id,
	TRIM(p.category) AS cleaned_category,
	TRY_CAST(TRIM(p.price) AS DECIMAL(10,2)) AS cleaned_price
FROM products AS p
),

-- CTE#5 filter CTE#2 for 2025 Q1 only
orders_2025 AS (
SELECT
	co.cleaned_order_id AS cleaned_order_id,
	co.cleaned_customer_id AS cleaned_customer_id,
	co.cleaned_order_date AS cleaned_order_date,
	co.cleaned_total_amount AS cleaned_total_amount
FROM cleaned_orders AS co
WHERE YEAR(co.cleaned_order_date) = 2025 AND MONTH(co.cleaned_order_date) <= 3
),

-- CTE#6 aggregate CTE#5
agg_orders_2025 AS (
SELECT
	o2025.cleaned_customer_id AS agg_customer_id,
	cc.cleaned_signup_date AS agg_signup_date,
	CASE
	WHEN COUNT(o2025.cleaned_order_id) > 1 THEN 'Yes'
	ELSE 'No'
	END AS agg_has_orders,
	COUNT(o2025.cleaned_order_id) AS agg_total_orders,
	SUM(o2025.cleaned_total_amount) AS agg_total_spent,
	MIN(o2025.cleaned_order_date) AS agg_first_order_date,
	CASE
	WHEN COUNT(o2025.cleaned_order_id) > 1 THEN 'Repeat Buyer'
	ELSE 'Non Repeat Buyer'
	END AS agg_repeat_buyer
FROM orders_2025 AS o2025
LEFT JOIN cleaned_customers AS cc ON o2025.cleaned_customer_id = cc.cleaned_customer_id
GROUP BY o2025.cleaned_customer_id, cc.cleaned_signup_date
),

-- CTE#7 ranking based on order count and choose top category
fav_category AS (
SELECT
	o2025.cleaned_customer_id as fc_customer_id,
	cp.cleaned_category as fc_category,
	ROW_NUMBER() OVER (PARTITION BY o2025.cleaned_customer_id ORDER BY COUNT(o2025.cleaned_order_id) DESC) AS fc_category_rank
FROM orders_2025 AS o2025
INNER JOIN cleaned_order_items as coi ON o2025.cleaned_order_id = coi.cleaned_order_id
INNER JOIN cleaned_products AS cp ON coi.cleaned_product_id = cp.cleaned_product_id
GROUP BY o2025.cleaned_customer_id, cp.cleaned_category
)

-- (MISTAKE HERE - refer to the comments) Final query to left join CTE#6 aggregates with the category by the customer id and filter for only category rank 1

--SELECT
--	ao2025.agg_customer_id AS customer_id,
--	ao2025.agg_signup_date AS signup_date,
--	ao2025.agg_has_orders AS has_orders,
--	ao2025.agg_total_orders AS total_orders,
--	ao2025.agg_total_spent AS total_spent,
--	ao2025.agg_first_order_date AS first_order_date,
--	fc.fc_category AS favourite_category,
--	ao2025.agg_repeat_buyer AS is_repeat_buyer
--FROM agg_orders_2025 AS ao2025
--LEFT JOIN fav_category AS fc ON ao2025.agg_customer_id = fc.fc_customer_id AND fc.fc_category_rank = 1

-- currently the final query would exclude customers that have yet to make any orders, which doesnt give the exact output we want
-- look back at the agg_orders_2025 it takes from the orders_2025 table which by default excludes customers who have yet to make any orders in Q1 2025
-- to fix this, the final query should be based on the cleaned_customers table and left join all the necessary fields (from the aggregate, category ranking etc)

-- the corrected final query should look like this with additional coalesce to return default values for those that are null
SELECT
    cc.cleaned_customer_id AS customer_id,
    cc.cleaned_signup_date AS signup_date,
    COALESCE(ao2025.agg_has_orders, 'No') AS has_orders,
    COALESCE(ao2025.agg_total_orders, 0) AS total_orders,
    COALESCE(ao2025.agg_total_spent, 0.00) AS total_spent,
    ao2025.agg_first_order_date AS first_order_date,
    fc.fc_category AS favourite_category,
    COALESCE(ao2025.agg_repeat_buyer, 'Non Repeat Buyer') AS is_repeat_buyer
FROM cleaned_customers AS cc
LEFT JOIN agg_orders_2025 AS ao2025 ON cc.cleaned_customer_id = ao2025.agg_customer_id
LEFT JOIN fav_category AS fc ON cc.cleaned_customer_id = fc.fc_customer_id AND fc.fc_category_rank = 1