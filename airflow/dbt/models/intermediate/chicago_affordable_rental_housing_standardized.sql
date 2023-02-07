{{ config(materialized='view') }}
{% set ck_cols = ["address", "management_company", "units", "property_type"] %}
{% set record_id = "housing_development_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
        upper(address::text)                            AS address,
        upper(zip_code::char(5))                        AS zip_code,
        upper(community_area::text)                     AS community_area,
        community_area_number::smallint                 AS community_area_number,
        upper(phone_number::text)                       AS phone_number,
        upper(property_type::text)                      AS property_type,
        upper(property_name::text)                      AS property_name,
        units::smallint                                 AS units,
        upper(management_company::text)                 AS management_company,
        x_coordinate::double precision                  AS x_coordinate,
        y_coordinate::double precision                  AS y_coordinate,
        latitude::double precision                      AS latitude,
        longitude::double precision                     AS longitude,
        geometry::GEOMETRY(GEOMETRY, 4326)              AS geometry,
        source_data_updated::timestamptz                AS source_data_updated,
        ingestion_check_time::timestamptz               AS ingestion_check_time
    FROM {{ ref('chicago_affordable_rental_housing') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
