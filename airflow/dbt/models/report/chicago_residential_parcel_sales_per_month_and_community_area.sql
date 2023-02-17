{{ config(materialized='table') }}
{% set min_count = 3 %}
{% set area_feat = "chicago_community" %}

WITH chicago_location_features AS (
	SELECT
		pin,
		{{ area_feat }},
		geometry
	FROM {{ ref('cook_county_parcel_locations_dim') }}
	WHERE property_city = 'CHICAGO'
),
chicago_residential_sales AS (
	SELECT
		ps.parcel_sale_id,
		ps.pin,
		to_char(ps.sale_date, 'YYYY-MM')       AS month_of_sale,
		ps.class,
		ps.class_descr,
		ps.sale_price,
		plf.{{ area_feat }}
	FROM {{ ref('cook_county_parcel_sales_fact') }} AS ps
	LEFT JOIN chicago_location_features AS plf
	ON ps.pin = plf.pin
	WHERE left(ps.class, 1) = '2'
),
sale_counts_per_group_per_month AS (
	SELECT
		count(*),
		month_of_sale
	FROM chicago_residential_sales
	GROUP BY month_of_sale
),
end_dates AS (
	SELECT to_char(month_of_sale, 'YYYY-MM') AS month_of_sale
	FROM generate_series(
		(
			SELECT (min(month_of_sale) || '-01')::date
			FROM sale_counts_per_group_per_month
			WHERE count >= {{ min_count }}
		),
		(
			SELECT (max(month_of_sale) || '-01')::date
			FROM sale_counts_per_group_per_month
			WHERE count >= {{ min_count }}
		),
		interval '1 month'
	) AS month_of_sale
),
sales_stats_per_group AS (
	SELECT
		month_of_sale,
		class,
		class_descr,
		{{ area_feat }},
		count(sale_price)                   AS num_sales,
		max(sale_price)                     AS max_sale_price,
		avg(sale_price)::bigint             AS mean_sale_price,
		min(sale_price)                     AS min_sale_price,
		percentile_cont(0.5) WITHIN GROUP (order by sale_price) AS median_sale_price	
	FROM chicago_residential_sales
	GROUP BY {{ area_feat }}, class_descr, class, month_of_sale
	ORDER BY {{ area_feat }}, month_of_sale, class
)

SELECT
	ed.month_of_sale,
	gss.class,
	gss.{{ area_feat }},
	COALESCE(gss.class_descr, 'DEPRECATED CLASS') AS class_descr,
	COALESCE(gss.num_sales, 0)                    AS num_sales,
	COALESCE(gss.max_sale_price, 0)               AS max_sale_price,
	COALESCE(gss.mean_sale_price, 0)              AS mean_sale_price,
	COALESCE(gss.min_sale_price, 0)               AS min_sale_price,
	COALESCE(gss.median_sale_price, 0)            AS median_sale_price
FROM end_dates AS ed
LEFT JOIN sales_stats_per_group AS gss
ON ed.month_of_sale = gss.month_of_sale
ORDER BY ed.month_of_sale, gss.{{ area_feat }}, gss.class