{{ config(materialized='view') }}
{% set ck_cols = ["linearid"] %}
{% set record_id = "linearid" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(linearid::text)                AS linearid,
        vintage_year::bigint                 AS vintage_year,
        upper(fullname::text)                AS fullname,
        upper(mtfcc::text)                   AS mtfcc,
        geometry::GEOMETRY(LINESTRING, 4269) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('united_states_rails_2022') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
