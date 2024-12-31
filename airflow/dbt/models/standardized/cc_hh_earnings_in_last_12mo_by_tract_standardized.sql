{{ config(materialized='view') }}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(geo_id::text)           AS geo_id,
        upper(name::text)             AS name,
        b19051_001e::bigint           AS b19051_001e,
        upper(b19051_001ea::text)     AS b19051_001ea,
        b19051_001m::bigint           AS b19051_001m,
        upper(b19051_001ma::text)     AS b19051_001ma,
        b19051_002e::bigint           AS b19051_002e,
        upper(b19051_002ea::text)     AS b19051_002ea,
        b19051_002m::bigint           AS b19051_002m,
        upper(b19051_002ma::text)     AS b19051_002ma,
        b19051_003e::bigint           AS b19051_003e,
        upper(b19051_003ea::text)     AS b19051_003ea,
        b19051_003m::bigint           AS b19051_003m,
        upper(b19051_003ma::text)     AS b19051_003ma,
        upper(state::text)            AS state,
        upper(county::text)           AS county,
        upper(tract::text)            AS tract,
        upper(dataset_base_url::text) AS dataset_base_url,
        dataset_id::bigint            AS dataset_id,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cc_hh_earnings_in_last_12mo_by_tract') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
