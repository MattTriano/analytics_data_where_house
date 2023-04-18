{{ config(materialized='view') }}
{% set ck_cols = ["id"] %}
{% set record_id = "id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        id::bigint                                           AS id,
        upper(case_number::text)                             AS case_number,
        date::timestamp AT TIME ZONE 'America/Chicago'       AS date,
        updated_on::timestamp AT TIME ZONE 'America/Chicago' AS updated_on,
        upper(primary_type::text)                            AS primary_type,
        upper(description::text)                             AS description,
        upper(iucr::text)                                    AS iucr,
        upper(fbi_code::text)                                AS fbi_code,
        upper(location_description::text)                    AS location_description,
        domestic::boolean                                    AS domestic,
        arrest::boolean                                      AS arrest,
        regexp_replace(district, '(^)0', '')                 AS district,
        ward::smallint                                       AS ward,
        upper(beat::text)                                    AS beat,
        upper(community_area::text)                          AS community_area,
        upper(block::text)                                   AS block,
        latitude::double precision                           AS latitude,
        longitude::double precision                          AS longitude,
        x_coordinate::bigint                                 AS x_coordinate,
        y_coordinate::bigint                                 AS y_coordinate,
        geometry::GEOMETRY(POINT, 4326)                      AS geometry,
        source_data_updated::timestamptz                     AS source_data_updated,
        ingestion_check_time::timestamptz                    AS ingestion_check_time
    FROM {{ ref('chicago_crimes') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
