{{ config(materialized='view') }}
{% set ck_cols = ["area_numbe"] %}
{% set record_id = "community_area_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
        area_numbe::smallint                   AS area_numbe,
        upper(community::text)                 AS community,        
        shape_len::double precision            AS shape_len,
        shape_area::double precision           AS shape_area,
        geometry::GEOMETRY(MULTIPOLYGON, 4326) AS geometry,
        source_data_updated::timestamptz       AS source_data_updated,
        ingestion_check_time::timestamptz      AS ingestion_check_time
    FROM {{ ref('chicago_community_area_boundaries') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
