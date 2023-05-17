{{ config(materialized='view') }}
{% set ck_cols = ["st_name", "f_street", "t_street", "br_ow_dir"] %}
{% set record_id = "bike_route_segment_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
       	upper(st_name::text)                      AS st_name,
        upper(f_street::text)                     AS f_street,
        upper(t_street::text)                     AS t_street,
        upper(street::text)                       AS street,
        upper(displayrou::text)                   AS displayrou,
        upper(oneway_dir::text)                   AS oneway_dir,
        upper(contraflow::text)                   AS contraflow,
        upper(br_ow_dir::text)                    AS br_ow_dir,
        upper(br_oneway::text)                    AS br_oneway,
        mi_ctrline::double precision              AS mi_ctrline,
        geometry::GEOMETRY(MULTILINESTRING, 4326) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_bike_paths') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
