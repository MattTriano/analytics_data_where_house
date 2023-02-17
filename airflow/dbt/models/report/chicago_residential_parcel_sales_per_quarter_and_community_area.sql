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
		to_char(ps.sale_date, 'YYYY-Q')       AS qtr_of_sale,
		ps.class,
		ps.class_descr,
		ps.sale_price,
		plf.{{ area_feat }}
	FROM {{ ref('cook_county_parcel_sales_fact') }} AS ps
	LEFT JOIN chicago_location_features AS plf
	ON ps.pin = plf.pin
	WHERE left(ps.class, 1) = '2'
),
sale_counts_per_group_per_qtr AS (
	SELECT
		count(*),
		qtr_of_sale
	FROM chicago_residential_sales
	GROUP BY qtr_of_sale
),
end_dates AS (
	SELECT to_char(qtr_of_sale, 'YYYY-Q') AS qtr_of_sale
	FROM generate_series(
		(
			SELECT (date_part('year', min(to_date(qtr_of_sale, 'YYYY-Q'))) || '-01-01')::date
			FROM sale_counts_per_group_per_qtr
			WHERE count >= {{ min_count }}
		),
		(
			SELECT (date_part('year', max(to_date(qtr_of_sale, 'YYYY-Q'))) || '-01-01')::date
			FROM sale_counts_per_group_per_qtr
			WHERE count >= {{ min_count }}
		),
		interval '3 month'
	) AS qtr_of_sale
),
sales_stats_per_group AS (
	SELECT
		qtr_of_sale,
		class,
		class_descr,
		{{ area_feat }},
		count(sale_price)                   AS num_sales,
		max(sale_price)                     AS max_sale_price,
		avg(sale_price)::bigint             AS mean_sale_price,
		min(sale_price)                     AS min_sale_price,
		percentile_cont(0.5) WITHIN GROUP (order by sale_price) AS median_sale_price	
	FROM chicago_residential_sales
	GROUP BY {{ area_feat }}, class_descr, class, qtr_of_sale
	ORDER BY {{ area_feat }}, qtr_of_sale, class
)

SELECT
	ed.qtr_of_sale,
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
ON ed.qtr_of_sale = gss.qtr_of_sale
ORDER BY ed.qtr_of_sale, gss.{{ area_feat }}, gss.class