{{ config(materialized='table') }}
{% set min_count_in_period = 2 %}

WITH sale_counts_per_group_per_qtr AS (
	SELECT
		count(*),
		to_char(sale_date, 'YYYY-Q') AS qtr_of_sale
	FROM {{ ref('cook_county_parcel_sales_fact') }}
	GROUP BY qtr_of_sale
),
end_dates AS (
	SELECT to_char(qtr_of_sale, 'YYYY-Q') AS qtr_of_sale
	FROM generate_series(
		(
			SELECT (date_part('year', min(to_date(qtr_of_sale, 'YYYY-Q'))) || '-01-01')::date
			FROM sale_counts_per_group_per_qtr
			WHERE count >= {{ min_count_in_period }}
		),
		(
			SELECT (date_part('year', max(to_date(qtr_of_sale, 'YYYY-Q'))) || '-01-01')::date
			FROM sale_counts_per_group_per_qtr
			WHERE count >= {{ min_count_in_period }}
		),
		interval '3 months'
	) AS qtr_of_sale
),
sales_stats_per_group AS (
	SELECT
		to_char(sale_date, 'YYYY-Q')       AS qtr_of_sale,
		class,
		class_descr,
		count(sale_price)                   AS num_sales,
		max(sale_price)                     AS max_sale_price,
		avg(sale_price)::bigint             AS mean_sale_price,
		min(sale_price)                     AS min_sale_price,
		percentile_cont(0.5) WITHIN GROUP (order by sale_price) AS median_sale_price	
	FROM {{ ref('cook_county_parcel_sales_fact') }}
	GROUP BY class_descr, class, qtr_of_sale
	ORDER BY qtr_of_sale, class
)

SELECT
	ed.qtr_of_sale,
	gss.class,
	COALESCE(gss.class_descr, 'DEPRECATED CLASS') AS class_descr,
	COALESCE(gss.num_sales, 0)                    AS num_sales,
	COALESCE(gss.max_sale_price, 0)               AS max_sale_price,
	COALESCE(gss.mean_sale_price, 0)              AS mean_sale_price,
	COALESCE(gss.min_sale_price, 0)               AS min_sale_price,
	COALESCE(gss.median_sale_price, 0)            AS median_sale_price
FROM end_dates AS ed
LEFT JOIN sales_stats_per_group AS gss
ON ed.qtr_of_sale = gss.qtr_of_sale
ORDER BY ed.qtr_of_sale, gss.class