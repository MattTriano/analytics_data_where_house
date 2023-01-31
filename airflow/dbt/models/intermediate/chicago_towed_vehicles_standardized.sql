{{ config(materialized='view') }}
{% set ck_cols = ["inventory_number", "tow_date", "plate"] %}
{% set record_id = "vehicle_tow_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
        upper(inventory_number::text) AS inventory_number,
        to_date(upper(tow_date::text), 'MM/DD/YYYY') AS tow_date,
        upper(make::varchar(4)) AS make,
        upper(model::varchar(3)) AS model,
        upper(style::varchar(2)) AS style,        
        upper(color::char(3)) AS color,
        upper(plate::varchar(10)) AS plate,
        upper(state::char(2)) AS state,
        upper(towed_to_address::text) AS towed_to_address,
        regexp_replace(upper(tow_facility_phone::text), '[^0-9]', '', 'g') AS tow_facility_phone,
        source_data_updated::timestamptz AS source_data_updated,
        ingestion_check_time::timestamptz AS ingestion_check_time
    FROM {{ ref('chicago_towed_vehicles') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
