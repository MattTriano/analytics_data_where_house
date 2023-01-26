{{ config(materialized='view') }}
{% set ck_cols = [
        REPLACE_WITH_COMPOSITE_KEY_COLUMNS
] %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS REPLACE_WITH_BETTER_id,
        upper(location_state::text) AS location_state,
        upper(location_zip::text) AS location_zip,
        upper(station_descriptive_name::text) AS station_descriptive_name,
        upper(blue::text) AS blue,
        upper(station_name::text) AS station_name,
        upper(y::text) AS y,
        upper(location_address::text) AS location_address,
        upper(location_city::text) AS location_city,
        upper(brn::text) AS brn,
        upper(direction_id::text) AS direction_id,
        upper(map_id::text) AS map_id,
        upper(g::text) AS g,
        upper(stop_name::text) AS stop_name,
        upper(p::text) AS p,
        upper(ada::text) AS ada,
        upper(pnk::text) AS pnk,
        upper(pexp::text) AS pexp,
        upper(stop_id::text) AS stop_id,
        upper(red::text) AS red,
        upper(o::text) AS o,
        geometry::GEOMETRY(POINT,4326) AS geometry,
        source_data_updated::timestamptz AS source_data_updated,
        ingestion_check_time::timestamptz AS ingestion_check_time
    FROM {{ ref('chicago_cta_train_stations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
