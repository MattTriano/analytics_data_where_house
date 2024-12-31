{{ config(materialized='view') }}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(geo_id::text)           AS geo_id,
        upper(name::text)             AS name,
        b19001_001e::bigint           AS b19001_001e,
        upper(b19001_001ea::text)     AS b19001_001ea,
        b19001_001m::bigint           AS b19001_001m,
        upper(b19001_001ma::text)     AS b19001_001ma,
        b19001_002e::bigint           AS b19001_002e,
        upper(b19001_002ea::text)     AS b19001_002ea,
        b19001_002m::bigint           AS b19001_002m,
        upper(b19001_002ma::text)     AS b19001_002ma,
        b19001_003e::bigint           AS b19001_003e,
        upper(b19001_003ea::text)     AS b19001_003ea,
        b19001_003m::bigint           AS b19001_003m,
        upper(b19001_003ma::text)     AS b19001_003ma,
        b19001_004e::bigint           AS b19001_004e,
        upper(b19001_004ea::text)     AS b19001_004ea,
        b19001_004m::bigint           AS b19001_004m,
        upper(b19001_004ma::text)     AS b19001_004ma,
        b19001_005e::bigint           AS b19001_005e,
        upper(b19001_005ea::text)     AS b19001_005ea,
        b19001_005m::bigint           AS b19001_005m,
        upper(b19001_005ma::text)     AS b19001_005ma,
        b19001_006e::bigint           AS b19001_006e,
        upper(b19001_006ea::text)     AS b19001_006ea,
        b19001_006m::bigint           AS b19001_006m,
        upper(b19001_006ma::text)     AS b19001_006ma,
        b19001_007e::bigint           AS b19001_007e,
        upper(b19001_007ea::text)     AS b19001_007ea,
        b19001_007m::bigint           AS b19001_007m,
        upper(b19001_007ma::text)     AS b19001_007ma,
        b19001_008e::bigint           AS b19001_008e,
        upper(b19001_008ea::text)     AS b19001_008ea,
        b19001_008m::bigint           AS b19001_008m,
        upper(b19001_008ma::text)     AS b19001_008ma,
        b19001_009e::bigint           AS b19001_009e,
        upper(b19001_009ea::text)     AS b19001_009ea,
        b19001_009m::bigint           AS b19001_009m,
        upper(b19001_009ma::text)     AS b19001_009ma,
        b19001_010e::bigint           AS b19001_010e,
        upper(b19001_010ea::text)     AS b19001_010ea,
        b19001_010m::bigint           AS b19001_010m,
        upper(b19001_010ma::text)     AS b19001_010ma,
        b19001_011e::bigint           AS b19001_011e,
        upper(b19001_011ea::text)     AS b19001_011ea,
        b19001_011m::bigint           AS b19001_011m,
        upper(b19001_011ma::text)     AS b19001_011ma,
        b19001_012e::bigint           AS b19001_012e,
        upper(b19001_012ea::text)     AS b19001_012ea,
        b19001_012m::bigint           AS b19001_012m,
        upper(b19001_012ma::text)     AS b19001_012ma,
        b19001_013e::bigint           AS b19001_013e,
        upper(b19001_013ea::text)     AS b19001_013ea,
        b19001_013m::bigint           AS b19001_013m,
        upper(b19001_013ma::text)     AS b19001_013ma,
        b19001_014e::bigint           AS b19001_014e,
        upper(b19001_014ea::text)     AS b19001_014ea,
        b19001_014m::bigint           AS b19001_014m,
        upper(b19001_014ma::text)     AS b19001_014ma,
        b19001_015e::bigint           AS b19001_015e,
        upper(b19001_015ea::text)     AS b19001_015ea,
        b19001_015m::bigint           AS b19001_015m,
        upper(b19001_015ma::text)     AS b19001_015ma,
        b19001_016e::bigint           AS b19001_016e,
        upper(b19001_016ea::text)     AS b19001_016ea,
        b19001_016m::bigint           AS b19001_016m,
        upper(b19001_016ma::text)     AS b19001_016ma,
        b19001_017e::bigint           AS b19001_017e,
        upper(b19001_017ea::text)     AS b19001_017ea,
        b19001_017m::bigint           AS b19001_017m,
        upper(b19001_017ma::text)     AS b19001_017ma,
        upper(state::text)            AS state,
        upper(county::text)           AS county,
        upper(tract::text)            AS tract,
        upper(dataset_base_url::text) AS dataset_base_url,
        dataset_id::bigint            AS dataset_id,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cc_hh_income_in_last_12mo_by_tract') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
