{{ config(materialized='view') }}
{% set ck_cols = ["town_nbhd", "triad_code"] %}
{% set record_id = "nbhd_boundary_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(town_nbhd::text)                                AS town_nbhd,
        upper(triad_code::char(1))                            AS triad_code,
        upper(triad_name::text)                               AS triad_name,
        lpad(upper(township_code::char(2)), 2, '0')           AS township_code,
        lpad(upper(nbhd::char(3)), 3, '0')                    AS nbhd,
        upper(township_name::text)                            AS township_name,
        geometry::GEOMETRY(MULTIPOLYGON,4326)                 AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cook_county_neighborhood_boundaries') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
