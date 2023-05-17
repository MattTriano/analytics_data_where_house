{{ config(materialized='view') }}
{% set ck_cols = ["district_name"] %}
{% set record_id = "district_name" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(district_name::text)        AS district_name,
        CASE
            WHEN upper(district) = 'HEADQUARTERS' THEN 'HQ'
            ELSE lpad(upper(district::char(2)), 2, '0')
        END                               AS district,        
        upper(address::text)              AS address,        
        upper(city::text)                 AS city,
        upper(zip::text)                  AS zip,
        upper(state::text)                AS state,        
        upper(phone::text)                AS phone,
        upper(tty::text)                  AS tty,
        upper(fax::text)                  AS fax,
        upper(website::text)              AS website,
        x_coordinate::double precision    AS x_coordinate,
        y_coordinate::double precision    AS y_coordinate,
        latitude::double precision        AS latitude,
        longitude::double precision       AS longitude,
        geometry::GEOMETRY(POINT, 4326)   AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_police_stations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
