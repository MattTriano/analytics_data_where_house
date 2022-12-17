{{ config(materialized='table') }}

WITH last_parcel_sale AS (
    SELECT
        parcel_sale_id,
        pin,
        is_multisale,
        num_parcels_sale,
        sale_price,
        sale_date,
        last_sale_price,
        last_sale_date,
        price_change_since_last_sale,
        years_since_last_sale,
        last_sale_was_multisale,
        num_parcels_last_sale
    FROM {{ ref('cook_county_parcel_price_change_between_sales') }}
),
sale_details AS (
    SELECT
        parcel_sale_id,
        sale_document_num,
        is_mydec_date,
        sale_deed_type,
        sale_type,
        class,
        township_code,
        sale_seller_name,    
        sale_buyer_name,    
        source_data_updated,
        ingestion_check_time
    FROM {{ ref('cook_county_parcel_sales_clean') }}
)

SELECT
    ls.parcel_sale_id,
    ls.pin,
    ls.is_multisale,
    ls.num_parcels_sale,
    ls.sale_price,
    ls.sale_date,
    ls.last_sale_price,
    ls.last_sale_date,
    ls.price_change_since_last_sale,
    ls.years_since_last_sale,
    ls.last_sale_was_multisale,
    ls.num_parcels_last_sale,
    sd.sale_document_num,
    sd.is_mydec_date,
    sd.sale_deed_type,
    sd.sale_type,
    sd.class,
    sd.township_code,
    sd.sale_seller_name,
    sd.sale_buyer_name,
    sd.source_data_updated,
    sd.ingestion_check_time
FROM last_parcel_sale AS ls
FULL OUTER JOIN sale_details AS sd
    ON ls.parcel_sale_id = sd.parcel_sale_id