{{ config(materialized='view') }}
{% set ck_cols = ["dist_num"] %}
{% set record_id = "dist_num" %}

WITH records_with_basic_cleaning AS (
    SELECT
        lpad(upper(dist_num::char(2)), 2, '0') AS dist_num,
        upper(dist_label::text)                AS dist_label,        
        geometry::GEOMETRY(MULTIPOLYGON, 4326) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_police_district_boundaries') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
WHERE {{ record_id }} <> '31'
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
