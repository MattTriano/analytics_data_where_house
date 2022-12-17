{{ config(materialized='view') }}

WITH last_parcel_sale AS (
    SELECT
        parcel_sale_id,
        pin,
        is_multisale,
        num_parcels_sale,
        sale_date,
        sale_price,
        LAG(sale_date, 1) OVER (PARTITION BY pin ORDER BY pin ASC, sale_date ASC) AS last_sale_date,
        LAG(sale_price, 1) OVER (PARTITION BY pin ORDER BY pin ASC, sale_date ASC) AS last_sale_price,
        LAG(is_multisale, 1) OVER (PARTITION BY pin ORDER BY pin ASC, sale_date ASC) AS last_sale_was_multisale,
        LAG(num_parcels_sale, 1) OVER (PARTITION BY pin ORDER BY pin ASC, sale_date ASC) AS num_parcels_last_sale
    FROM {{ ref('cook_county_parcel_sales_clean') }}
),
price_change_since_last_sale AS (
    SELECT
        parcel_sale_id,
        pin,
        is_multisale,
        num_parcels_sale,
        sale_price,
        sale_date,
        last_sale_price,
        last_sale_date,
        sale_price - last_sale_price AS price_change_since_last_sale,
        (sale_date - last_sale_date) / 365.2422 AS years_since_last_sale,
        last_sale_was_multisale,
        num_parcels_last_sale
    FROM last_parcel_sale
)

SELECT *
FROM price_change_since_last_sale
ORDER BY pin, sale_date
