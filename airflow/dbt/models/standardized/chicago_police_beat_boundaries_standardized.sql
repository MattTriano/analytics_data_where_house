{{ config(materialized='view') }}
{% set ck_cols = ["beat_num"] %}
{% set record_id = "beat_num" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(beat_num::text)                  AS beat_num,
        upper(beat::char(1))                   AS beat,        
        upper(district::char(2))               AS district,
        upper(sector::char(1))                 AS sector,
        geometry::GEOMETRY(MULTIPOLYGON, 4326) AS geometry,
        source_data_updated::timestamptz       AS source_data_updated,
        ingestion_check_time::timestamptz      AS ingestion_check_time
    FROM {{ ref('chicago_police_beat_boundaries') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
WHERE {{ record_id }} <> '3100'
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
