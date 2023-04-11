{{ config(materialized='view') }}
{% set ck_cols = ["systemstop"] %}
{% set record_id = "systemstop" %}

WITH records_with_basic_cleaning AS (
    SELECT
        systemstop::smallint              AS systemstop,
        upper(public_nam::text)           AS public_name,
        upper(dir::text)                  AS dir,
        upper(street::text)               AS street,
        upper(cross_st::text)             AS cross_st,
        upper(city::text)                 AS city,
        upper(routesstpg::text)           AS routesstpg,
        upper(owlroutes::text)            AS owlroutes,
        upper(pos::text)                  AS pos,
        geometry::GEOMETRY(POINT, 4326)   AS geometry,
        source_data_updated::timestamptz  AS source_data_updated,
        ingestion_check_time::timestamptz AS ingestion_check_time
    FROM {{ ref('chicago_cta_bus_stops') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated