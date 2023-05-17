{{ config(materialized='view') }}
{% set ck_cols = ["objectid"] %}
{% set record_id = "objectid" %}

WITH records_with_basic_cleaning AS (
    SELECT
        objectid::smallint                     AS objectid,
        upper(name::text)                      AS name,
        shape_area::double precision           AS shape_area,
        shape_len::double precision            AS shape_len,
        geometry::GEOMETRY(MULTIPOLYGON, 4326) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_city_boundary') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
