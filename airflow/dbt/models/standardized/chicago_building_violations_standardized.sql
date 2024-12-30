{{ config(materialized='view') }}
{% set ck_cols = ["id"] %}
{% set record_id = "id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        id::bigint                                AS id,
        violation_date::date                      AS violation_date,
        upper(violation_code::text)               AS violation_code,
        upper(violation_description::text)        AS violation_description,
        upper(violation_status::text)             AS violation_status,
        violation_status_date::date               AS violation_status_date,
        upper(violation_location::text)           AS violation_location,
        upper(violation_ordinance::text)          AS violation_ordinance,
        violation_last_modified_date::timestamptz AT TIME ZONE 'UTC'
            AT TIME ZONE 'America/Chicago'        AS violation_last_modified_date,
        upper(inspection_number::text)            AS inspection_number,
        upper(inspector_id::text)                 AS inspector_id,
        upper(violation_inspector_comments::text) AS violation_inspector_comments,
        upper(inspection_waived::text)            AS inspection_waived,
        upper(inspection_category::text)          AS inspection_category,
        upper(inspection_status::text)            AS inspection_status,
        upper(department_bureau::text)            AS department_bureau,
        upper(property_group::text)               AS property_group,
        upper(ssa::text)                          AS ssa,
        upper(address::text)                      AS address,
        upper(street_number::text)                AS street_number,
        upper(street_direction::text)             AS street_direction,
        upper(street_name::text)                  AS street_name,
        upper(street_type::text)                  AS street_type,
        longitude::double precision               AS longitude,
        latitude::double precision                AS latitude,
        geometry::GEOMETRY(POINT, 4326)           AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_building_violations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
