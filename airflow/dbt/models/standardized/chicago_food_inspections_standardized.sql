{{ config(materialized='view') }}
{% set ck_cols = ["inspection_id"] %}
{% set record_id = "inspection_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        inspection_id::int                                    AS inspection_id,
        license_::int                                         AS license_id,
        upper(dba_name::text)                                 AS dba_name,
        upper(aka_name::text)                                 AS aka_name,
        inspection_date::date                                 AS inspection_date,
        upper(inspection_type::text)                          AS inspection_type,
        upper(violations::text)                               AS violations,
        upper(results::text)                                  AS results,
        upper(facility_type::text)                            AS facility_type,
        upper(risk::text)                                     AS risk,
        upper(address::text)                                  AS address,
        regexp_replace(upper(city::text), '[^A-Z ]', '', 'g') AS city,
        upper(zip::char(5))                                   AS zip,
        upper(state::char(2))                                 AS state,
        latitude::double precision                            AS latitude,
        longitude::double precision                           AS longitude,
        geometry::GEOMETRY(GEOMETRY, 4326)                    AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_food_inspections') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
