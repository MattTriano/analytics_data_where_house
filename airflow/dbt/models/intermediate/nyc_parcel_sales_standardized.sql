{{ config(materialized='view') }}
{% set ck_cols = ["address", "lot", "sale_price", "block", "sale_date"] %}
{% set record_id = "nyc_parcel_sale_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }}           AS {{ record_id }},
        regexp_replace(sale_price::text, ',', '', 'g')::BIGINT    AS sale_price, 
        sale_date::date                                           AS sale_date,
        block::smallint                                           AS block,
        lot::smallint                                             AS lot,
        borough::smallint                                         AS borough,
        upper(address::text)                                      AS address,
        upper(apartment_number::text)                             AS apartment_number,
        upper(zip_code::text)                                     AS zip_code,        
        upper(neighborhood::text)                                 AS neighborhood,
        upper(building_class_category::text)                      AS building_class_category,
        upper(tax_class_at_time_of_sale::text)                    AS tax_class_at_time_of_sale,
        upper(building_class_at_time_of_sale::text)               AS building_class_at_time_of_sale,
        upper(tax_class_at_present::text)                         AS tax_class_at_present,
        upper(building_class_at_present::text)                    AS building_class_at_present,
        residential_units::smallint                               AS residential_units,
        commercial_units::smallint                                AS commercial_units,
        total_units::smallint                                     AS total_units,
        regexp_replace(land_square_feet::text, ',', '', 'g')::int AS land_square_feet,
        gross_square_feet::int                                    AS gross_square_feet,
        to_date(year_built::VARCHAR, 'yyyy')                      AS year_built,
        source_data_updated::timestamptz                          AS source_data_updated,
        ingestion_check_time::timestamptz                         AS ingestion_check_time
    FROM {{ ref('nyc_parcel_sales') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
