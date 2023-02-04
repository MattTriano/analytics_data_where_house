{{ config(materialized='view') }}
{% set ck_cols = ["unique_id"] %}
{% set record_id = "unique_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(unique_id::text)                      AS unique_id,
        date::timestamp                             AS date,
        lpad(hour::char(2), 2, '0')                 AS hour,
        day_of_week::smallint                       AS day_of_week,
        lpad(month::char(2), 2, '0')                AS month,        
        upper(incident_type_description::text)      AS incident_type_description,
        rounds::smallint                            AS rounds,
        upper(block::text)                          AS block,
        upper(zip_code::char(5))                    AS zip_code,
        upper(community_area::text)                 AS community_area,
        lpad(upper(beat::char(4)), 4, '0')          AS beat,
        lpad(upper(district::char(2)), 2, '0')      AS district,
        upper(area::char(1))                        AS area,
        ward::smallint                              AS ward,
        upper(illinois_house_district::varchar(3))  AS illinois_house_district,
        upper(illinois_senate_district::varchar(2)) AS illinois_senate_district,
        upper(street_outreach_organization::text)   AS street_outreach_organization,
        latitude::double precision                  AS latitude,
        longitude::double precision                 AS longitude,
        geometry::GEOMETRY(POINT, 4326)             AS geometry,
        source_data_updated::timestamptz            AS source_data_updated,
        ingestion_check_time::timestamptz           AS ingestion_check_time
    FROM {{ ref('chicago_shotspotter_alerts') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
