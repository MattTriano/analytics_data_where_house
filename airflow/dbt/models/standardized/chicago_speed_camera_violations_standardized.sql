{{ config(materialized='view') }}
{% set ck_cols = ["camera_id", "violation_date"] %}
{% set record_id = "camera_violations_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(camera_id::text)         AS camera_id,
        violation_date::date           AS violation_date,
        violations::bigint             AS violations,
        upper(address::text)           AS address,
        x_coordinate::double precision AS x_coordinate,
        y_coordinate::double precision AS y_coordinate,
        longitude::double precision    AS longitude,
        latitude::double precision     AS latitude,
        geometry::GEOMETRY(POINT,4326) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_speed_camera_violations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
