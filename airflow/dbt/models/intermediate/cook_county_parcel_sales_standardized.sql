{{ config(materialized='view') }}
{% set ck_cols = [
     "pin", "sale_document_num", "sale_date", "sale_price",
] %}

WITH records_with_basic_cleaning AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(ck_cols) }}   AS parcel_sale_id,
    pin::bigint                                       AS pin,
    upper(sale_document_num::text)                    AS sale_document_num,
    sale_date::date                                   AS sale_date,
    is_mydec_date::boolean                            AS is_mydec_date,
    year::int                                         AS year,
    sale_price::bigint                                AS sale_price,
    upper(sale_deed_type::text)                       AS sale_deed_type,
    upper(sale_type::text)                            AS sale_type,
    upper(class::varchar(6))                          AS class,
    township_code::bigint                             AS township_code,
    is_multisale::boolean                             AS is_multisale,
    num_parcels_sale::int                             AS num_parcels_sale,    
    upper(sale_seller_name::text)                     AS sale_seller_name,    
    upper(sale_buyer_name::text)                      AS sale_buyer_name,    
    source_data_updated::timestamptz                  AS source_data_updated,
    ingestion_check_time::timestamptz                 AS ingestion_check_time
  FROM {{ ref('cook_county_parcel_sales') }}
  ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
