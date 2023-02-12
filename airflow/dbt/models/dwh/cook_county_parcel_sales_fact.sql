{{ config(materialized='table') }}

WITH sale_features AS (
    SELECT
        parcel_sale_id,
        pin,
        class,
        class_descr,
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
    FROM {{ ref('cook_county_parcel_sales_feature') }}
),
sale_details AS (
    SELECT
        parcel_sale_id,
        sale_document_num,
        is_mydec_date,
        sale_deed_type,
        sale_type,
        township_code,
        sale_seller_name,    
        sale_buyer_name,    
        source_data_updated,
        ingestion_check_time
    FROM {{ ref('cook_county_parcel_sales_clean') }}
)

SELECT
    sf.parcel_sale_id,
    sf.pin,
    sf.class,
    sf.class_descr,
    sf.is_multisale,
    sf.num_parcels_sale,
    sf.sale_price,
    sf.sale_date,
    sf.last_sale_price,
    sf.last_sale_date,
    sf.price_change_since_last_sale,
    sf.years_since_last_sale,
    sf.last_sale_was_multisale,
    sf.num_parcels_last_sale,
    sd.sale_document_num,
    sd.is_mydec_date,
    sd.sale_deed_type,
    sd.sale_type,
    sd.township_code,
    sd.sale_seller_name,
    sd.sale_buyer_name,
    sd.source_data_updated,
    sd.ingestion_check_time
FROM sale_features AS sf
FULL OUTER JOIN sale_details AS sd
    ON sf.parcel_sale_id = sd.parcel_sale_id