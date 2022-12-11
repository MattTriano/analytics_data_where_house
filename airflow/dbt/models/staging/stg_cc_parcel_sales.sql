{{ config(materialized='view') }}

WITH parcel_sales_data AS
(
  SELECT *, row_number() over(partition by pin, sale_document_num, sale_date, sale_price) as rn
  FROM {{ source('staging','temp_cook_county_parcel_sales') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['pin', 'sale_document_num', 'sale_date', 'sale_price']) }} AS parcel_sale_id,
    pin::bigint AS pin,
    year::int AS year,
    township_code::bigint AS township_code,
    upper(class::varchar(6)) AS class,
    sale_date::timestamp AS sale_date,
    is_mydec_date::boolean AS is_mydec_date,
    sale_price::bigint AS sale_price,
    upper(sale_document_num::text) AS sale_document_num,
    upper(sale_deed_type::text) AS sale_deed_type,
    upper(sale_seller_name::text) AS sale_seller_name,
    is_multisale::boolean AS is_multisale,
    num_parcels_sale::int AS num_parcels_sale,
    upper(sale_buyer_name::text) AS sale_buyer_name,
    upper(sale_type::text) AS sale_type,
    source_data_updated::text AS source_data_updated,
    ingestion_check_time::text AS ingestion_check_time
FROM parcel_sales_data
WHERE rn = 1

-- -- dbt build --m <model.sql> --var 'is_test_run: false'
-- {% if var('is_test_run', default=true) %}
--   limit 100
-- {% endif %}
