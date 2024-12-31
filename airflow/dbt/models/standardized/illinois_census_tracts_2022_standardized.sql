{{ config(materialized='view') }}
{% set ck_cols = ["REPLACE_WITH_COMPOSITE_KEY_COLUMNS"] %}
{% set record_id = "REPLACE_WITH_BETTER_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(geoid::text)                AS geoid,
        vintage_year::bigint              AS vintage_year,
        upper(statefp::text)              AS statefp,
        upper(countyfp::text)             AS countyfp,
        upper(tractce::text)              AS tractce,
        upper(name::text)                 AS name,
        upper(namelsad::text)             AS namelsad,
        upper(mtfcc::text)                AS mtfcc,
        upper(funcstat::text)             AS funcstat,
        aland::bigint                     AS area_land,
        awater::bigint                    AS area_water,
        intptlon::double precision.       AS longitude,
        intptlat::double precision        AS latitude,
        geometry::GEOMETRY(GEOMETRY,4269) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('illinois_census_tracts_2022') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
