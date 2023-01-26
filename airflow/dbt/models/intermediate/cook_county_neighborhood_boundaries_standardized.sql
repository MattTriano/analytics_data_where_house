{{ config(materialized='view') }}
{% set ck_cols = ["REPLACE_WITH_COMPOSITE_KEY_COLUMNS"] %}
{% set record_id = "REPLACE_WITH_BETTER_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
        upper(triad_name::text) AS triad_name,
        upper(town_nbhd::text) AS town_nbhd,
        upper(township_code::text) AS township_code,
        upper(triad_code::text) AS triad_code,
        upper(township_name::text) AS township_name,
        upper(nbhd::text) AS nbhd,
        geometry::GEOMETRY(MULTIPOLYGON,4326) AS geometry,
        source_data_updated::timestamptz AS source_data_updated,
        ingestion_check_time::timestamptz AS ingestion_check_time
    FROM {{ ref('cook_county_neighborhood_boundaries') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
