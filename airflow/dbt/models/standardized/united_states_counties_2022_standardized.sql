{{ config(materialized='view') }}
{% set ck_cols = ["REPLACE_WITH_COMPOSITE_KEY_COLUMNS"] %}
{% set record_id = "REPLACE_WITH_BETTER_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(statefp::text) AS statefp,
        upper(countyfp::text) AS countyfp,
        upper(countyns::text) AS countyns,
        upper(geoid::text) AS geoid,
        upper(name::text) AS name,
        upper(namelsad::text) AS namelsad,
        upper(lsad::text) AS lsad,
        upper(classfp::text) AS classfp,
        upper(mtfcc::text) AS mtfcc,
        csafp::double precision AS csafp,
        cbsafp::double precision AS cbsafp,
        metdivfp::double precision AS metdivfp,
        upper(funcstat::text) AS funcstat,
        aland::bigint AS aland,
        awater::bigint AS awater,
        upper(intptlat::text) AS intptlat,
        upper(intptlon::text) AS intptlon,
        geometry::GEOMETRY(GEOMETRY,4269) AS geometry,
        vintage_year::bigint AS vintage_year,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('united_states_counties_2022') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
