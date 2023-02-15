# Engineering Temporal (Time Series) Features

```sql
SELECT
	to_char(sale_date, 'YYYY-MM')       AS month_of_sale,
	count(sale_price)                   AS num_sales,
	max(sale_price)                     AS max_sale_price,
	avg(sale_price)::bigint             AS mean_sale_price,
	min(sale_price)                     AS min_sale_price,
	percentile_cont(0.5) WITHIN GROUP (order by sale_price) AS median_sale_price	
FROM feature.cook_county_parcel_price_change_between_sales
GROUP BY month_of_sale
ORDER BY month_of_sale
```

## Generating an evenly spaced series of dates

```sql
WITH sale_counts AS (
	SELECT
		count(*),
		to_char(sale_date, 'YYYY-MM')       AS month_of_sale
	FROM dwh.cook_county_parcel_sales_fact
	GROUP BY month_of_sale
),
end_dates AS (
	SELECT *
	FROM generate_series(
		(SELECT (min(month_of_sale) || '-01')::date FROM sale_counts WHERE count >= 5),
		(SELECT (max(month_of_sale) || '-01')::date FROM sale_counts WHERE count >= 5),
		interval '1 month'
	)		
)

SELECT * FROM end_dates
```

Syntax decoder: `||` is the same as `concat()`