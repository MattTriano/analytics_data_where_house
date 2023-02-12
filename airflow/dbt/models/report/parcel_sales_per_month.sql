{{ config(materialized='table') }}

SELECT
	to_char(sale_date, 'YYYY-MM')       AS month_of_sale,
	count(sale_price)                   AS num_sales,
	max(sale_price)                     AS max_sale_price,
	avg(sale_price)::bigint             AS mean_sale_price,
	min(sale_price)                     AS min_sale_price,
	percentile_cont(0.5) WITHIN GROUP (order by sale_price) AS median_sale_price	
FROM {{ ref('cook_county_parcel_sales_fact') }}
GROUP BY month_of_sale
ORDER BY month_of_sale