{{ config(materialized='view') }}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(geo_id::text)           AS geo_id,
        upper(name::text)             AS name,
        b25003_001e::bigint           AS b25003_001e,
        upper(b25003_001ea::text)     AS b25003_001ea,
        b25003_001m::bigint           AS b25003_001m,
        upper(b25003_001ma::text)     AS b25003_001ma,
        b25003_002e::bigint           AS b25003_002e,
        upper(b25003_002ea::text)     AS b25003_002ea,
        b25003_002m::bigint           AS b25003_002m,
        upper(b25003_002ma::text)     AS b25003_002ma,
        b25003_003e::bigint           AS b25003_003e,
        upper(b25003_003ea::text)     AS b25003_003ea,
        b25003_003m::bigint           AS b25003_003m,
        upper(b25003_003ma::text)     AS b25003_003ma,
        b25003a_001e::bigint          AS b25003a_001e,
        upper(b25003a_001ea::text)    AS b25003a_001ea,
        b25003a_001m::bigint          AS b25003a_001m,
        upper(b25003a_001ma::text)    AS b25003a_001ma,
        b25003a_002e::bigint          AS b25003a_002e,
        upper(b25003a_002ea::text)    AS b25003a_002ea,
        b25003a_002m::bigint          AS b25003a_002m,
        upper(b25003a_002ma::text)    AS b25003a_002ma,
        b25003a_003e::bigint          AS b25003a_003e,
        upper(b25003a_003ea::text)    AS b25003a_003ea,
        b25003a_003m::bigint          AS b25003a_003m,
        upper(b25003a_003ma::text)    AS b25003a_003ma,
        b25003b_001e::bigint          AS b25003b_001e,
        upper(b25003b_001ea::text)    AS b25003b_001ea,
        b25003b_001m::bigint          AS b25003b_001m,
        upper(b25003b_001ma::text)    AS b25003b_001ma,
        b25003b_002e::bigint          AS b25003b_002e,
        upper(b25003b_002ea::text)    AS b25003b_002ea,
        b25003b_002m::bigint          AS b25003b_002m,
        upper(b25003b_002ma::text)    AS b25003b_002ma,
        b25003b_003e::bigint          AS b25003b_003e,
        upper(b25003b_003ea::text)    AS b25003b_003ea,
        b25003b_003m::bigint          AS b25003b_003m,
        upper(b25003b_003ma::text)    AS b25003b_003ma,
        b25003d_001e::bigint          AS b25003d_001e,
        upper(b25003d_001ea::text)    AS b25003d_001ea,
        b25003d_001m::bigint          AS b25003d_001m,
        upper(b25003d_001ma::text)    AS b25003d_001ma,
        b25003d_002e::bigint          AS b25003d_002e,
        upper(b25003d_002ea::text)    AS b25003d_002ea,
        b25003d_002m::bigint          AS b25003d_002m,
        upper(b25003d_002ma::text)    AS b25003d_002ma,
        b25003d_003e::bigint          AS b25003d_003e,
        upper(b25003d_003ea::text)    AS b25003d_003ea,
        b25003d_003m::bigint          AS b25003d_003m,
        upper(b25003d_003ma::text)    AS b25003d_003ma,
        upper(state::text)            AS state,
        upper(county::text)           AS county,
        upper(tract::text)            AS tract,
        upper(dataset_base_url::text) AS dataset_base_url,
        dataset_id::bigint            AS dataset_id,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cc_housing_tenure_single_race_by_tract_acs5') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
