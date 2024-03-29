{{ config(materialized='view') }}
{% set ck_cols = ["service_request_number"] %}
{% set record_id = "service_request_number" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(service_request_number::text)                   AS service_request_number,
        relocated_date::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS relocated_date,
        upper(relocated_reason::text)                         AS relocated_reason,
        upper(relocated_from_address_number::text)            AS relocated_from_address_number,
        upper(relocated_from_street_direction::text)          AS relocated_from_street_direction,
        upper(relocated_from_street_name::text)               AS relocated_from_street_name,
        upper(relocated_from_suffix::text)                    AS relocated_from_suffix,
        relocated_from_longitude::double precision            AS relocated_from_longitude,
        relocated_from_latitude::double precision             AS relocated_from_latitude,
        relocated_from_x_coordinate::double precision         AS relocated_from_x_coordinate,
        relocated_from_y_coordinate::double precision         AS relocated_from_y_coordinate,
        upper(relocated_to_address_number::text)              AS relocated_to_address_number,
        upper(relocated_to_direction::text)                   AS relocated_to_direction,
        upper(relocated_to_street_name::text)                 AS relocated_to_street_name,
        upper(relocated_to_suffix::text)                      AS relocated_to_suffix,
        upper(plate::text)                                    AS plate,
        upper(make::varchar(20))                              AS make,
        upper(state::char(2))                                 AS state,
        upper(color::varchar(12))                             AS color,
        geometry::GEOMETRY(POINT,4326)                        AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_relocated_vehicles') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
