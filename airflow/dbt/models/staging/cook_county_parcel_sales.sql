{{ config(materialized='table') }}

WITH current_pull_with_distinct_combos_numbered AS (
  SELECT *, 
         row_number() over(partition by pin, year, township_code, class, sale_date, is_mydec_date,
                                        sale_price, sale_document_num, sale_deed_type, 
                                        sale_seller_name, is_multisale, num_parcels_sale,
                                        sale_buyer_name, sale_type, source_data_updated, ingestion_check_time
          ) as rn
  FROM {{ source('staging','temp_cook_county_parcel_sales') }}
),
distinct_records_in_current_pull AS (
  SELECT *
  FROM current_pull_with_distinct_combos_numbered
  WHERE rn = 1
),
records_in_data_raw_table AS (
  SELECT *
  FROM {{ source('staging','cook_county_parcel_sales') }}
),
new_or_updated_records_in_current_pull AS (
  SELECT pin, year, township_code, class, sale_date, is_mydec_date, sale_price,
         sale_document_num, sale_deed_type, sale_seller_name, is_multisale,
         num_parcels_sale, sale_buyer_name, sale_type, source_data_updated, ingestion_check_time
  FROM distinct_records_in_current_pull new
  WHERE NOT EXISTS (
    SELECT
    FROM records_in_data_raw_table old
    WHERE ((new.pin = old.pin) OR
           (new.pin IS NULL AND old.pin IS NULL))
      AND ((new.year = old.year) OR
           (new.year IS NULL AND old.year IS NULL))
      AND ((new.township_code = old.township_code) OR
           (new.township_code IS NULL AND old.township_code IS NULL))
      AND ((new.class = old.class) OR
           (new.class IS NULL AND old.class IS NULL))
      AND ((new.sale_date = old.sale_date) OR
           (new.sale_date IS NULL AND old.sale_date IS NULL))
      AND ((new.is_mydec_date = old.is_mydec_date) OR
           (new.is_mydec_date IS NULL AND old.is_mydec_date IS NULL))
      AND ((new.sale_price = old.sale_price) OR
           (new.sale_price IS NULL AND old.sale_price IS NULL))
      AND ((new.sale_document_num = old.sale_document_num) OR
           (new.sale_document_num IS NULL AND old.sale_document_num IS NULL))
      AND ((new.sale_deed_type = old.sale_deed_type) OR
           (new.sale_deed_type IS NULL AND old.sale_deed_type IS NULL))
      AND ((new.sale_seller_name = old.sale_seller_name) OR
           (new.sale_seller_name IS NULL AND old.sale_seller_name IS NULL))
      AND ((new.is_multisale = old.is_multisale) OR
           (new.is_multisale IS NULL AND old.is_multisale IS NULL))
      AND ((new.num_parcels_sale = old.num_parcels_sale) OR
           (new.num_parcels_sale IS NULL AND old.num_parcels_sale IS NULL))
      AND ((new.sale_buyer_name = old.sale_buyer_name) OR
           (new.sale_buyer_name IS NULL AND old.sale_buyer_name IS NULL))
      AND ((new.sale_type = old.sale_type) OR
           (new.sale_type IS NULL AND old.sale_type IS NULL))
  )
),
data_raw_table_with_new_and_updated_records AS (
  SELECT * 
  FROM records_in_data_raw_table
    UNION ALL
  SELECT *
  FROM new_or_updated_records_in_current_pull
)

SELECT * 
FROM data_raw_table_with_new_and_updated_records
ORDER BY pin, year, sale_document_num
