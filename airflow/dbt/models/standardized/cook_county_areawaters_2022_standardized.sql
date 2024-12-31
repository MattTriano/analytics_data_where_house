{{ config(materialized='view') }}
{% set ck_cols = ["hydroid"] %}
{% set record_id = "hydroid" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(hydroid::text)              AS hydroid,
        vintage_year::bigint              AS vintage_year,
        ansicode::double precision        AS ansicode,
        upper(fullname::text)             AS fullname,
        upper(mtfcc::text)                AS mtfcc,
        aland::bigint                     AS area_land,
        awater::bigint                    AS area_water,
        intptlon::double precision        AS longitude,
        intptlat::double precision        AS latitude,
        geometry::GEOMETRY(POLYGON, 4269) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cook_county_areawaters_2022') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
