{{ config(materialized='view') }}
{% set ck_cols = ["id"] %}
{% set record_id = "id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(id::text)                                       AS id,
        upper(station_name::text)                             AS station_name,
        upper(short_name::text)                               AS short_name,
        upper(status::text)                                   AS status,
        latitude::double precision.                           AS latitude,
        longitude::double precision                           AS longitude,
        docks_in_service::bigint                              AS docks_in_service,
        total_docks::bigint                                   AS total_docks,
        geometry::GEOMETRY(POINT,4326)                        AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_divvy_stations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
