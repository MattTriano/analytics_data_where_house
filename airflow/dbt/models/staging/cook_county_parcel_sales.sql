{{ config(materialized='table') }}

WITH latest_temp_cook_county_parcel_sales_data AS (
  SELECT *, 
         row_number() over(partition by pin, year, township_code, class, sale_date, is_mydec_date,
                                        sale_price, sale_document_num, sale_deed_type, 
                                        sale_seller_name, is_multisale, num_parcels_sale,
                                        sale_buyer_name, sale_type, source_data_updated
          ) as rn
  FROM {{ source('staging','temp_cook_county_parcel_sales') }}
),
latest_distinct_temp_cook_county_parcel_sales_data AS (
  SELECT *
  FROM latest_temp_cook_county_parcel_sales_data
  WHERE rn = 1
),
data_raw_cook_county_parcel_sales_data AS (
  SELECT *
  FROM {{ source('staging','cook_county_parcel_sales') }}
),
new_parcel_sales_data_in_temp AS (
  SELECT pin, year, township_code, class, sale_date, is_mydec_date, sale_price,
         sale_document_num, sale_deed_type, sale_seller_name, is_multisale,
         num_parcels_sale, sale_buyer_name, sale_type, source_data_updated, ingestion_check_time
  FROM latest_distinct_temp_cook_county_parcel_sales_data stg
  WHERE NOT EXISTS (
    SELECT
    FROM data_raw_cook_county_parcel_sales_data old
    WHERE     stg.pin = old.pin AND
              stg.year = old.year AND
              stg.township_code = old.township_code AND
              stg.class = old.class AND 
              stg.sale_date = old.sale_date AND
              stg.is_mydec_date = old.is_mydec_date AND
              stg.sale_price = old.sale_price AND
              stg.sale_document_num = old.sale_document_num AND
              stg.sale_deed_type = old.sale_deed_type AND
              stg.sale_seller_name = old.sale_seller_name AND
              stg.is_multisale = old.is_multisale AND
              stg.num_parcels_sale = old.num_parcels_sale AND
              stg.sale_buyer_name = old.sale_buyer_name AND
              stg.sale_type = old.sale_type AND
              stg.source_data_updated = old.source_data_updated AND
              stg.ingestion_check_time = old.ingestion_check_time
  )
),
new_data_raw_cook_county_parcel_sales_data AS (
  SELECT * 
  FROM data_raw_cook_county_parcel_sales_data
    UNION ALL
  SELECT *
  FROM new_parcel_sales_data_in_temp
)

SELECT * 
FROM new_data_raw_cook_county_parcel_sales_data
