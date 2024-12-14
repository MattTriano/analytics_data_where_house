{{ config(materialized='view') }}
{% set ck_cols = ["description"] %}
{% set record_id = "description" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(description::text) AS description,
        upper(legend::text) AS legend,
        upper(type::text) AS type,
        upper(lines::text) AS lines,
        upper(shape_len::text) AS shape_len,
        geometry::GEOMETRY(MULTILINESTRING,4326) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_cta_train_lines') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
