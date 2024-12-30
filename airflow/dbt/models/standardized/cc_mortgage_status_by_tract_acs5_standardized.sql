{{ config(materialized='view') }}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(geo_id::text)           AS geo_id,
        upper(name::text)             AS name,
        b25081_001e::bigint           AS b25081_001e,
        upper(b25081_001ea::text)     AS b25081_001ea,
        b25081_001m::bigint           AS b25081_001m,
        upper(b25081_001ma::text)     AS b25081_001ma,
        b25081_002e::bigint           AS b25081_002e,
        upper(b25081_002ea::text)     AS b25081_002ea,
        b25081_002m::bigint           AS b25081_002m,
        upper(b25081_002ma::text)     AS b25081_002ma,
        b25081_003e::bigint           AS b25081_003e,
        upper(b25081_003ea::text)     AS b25081_003ea,
        b25081_003m::bigint           AS b25081_003m,
        upper(b25081_003ma::text)     AS b25081_003ma,
        b25081_004e::bigint           AS b25081_004e,
        upper(b25081_004ea::text)     AS b25081_004ea,
        b25081_004m::bigint           AS b25081_004m,
        upper(b25081_004ma::text)     AS b25081_004ma,
        b25081_005e::bigint           AS b25081_005e,
        upper(b25081_005ea::text)     AS b25081_005ea,
        b25081_005m::bigint           AS b25081_005m,
        upper(b25081_005ma::text)     AS b25081_005ma,
        b25081_006e::bigint           AS b25081_006e,
        upper(b25081_006ea::text)     AS b25081_006ea,
        b25081_006m::bigint           AS b25081_006m,
        upper(b25081_006ma::text)     AS b25081_006ma,
        b25081_007e::bigint           AS b25081_007e,
        upper(b25081_007ea::text)     AS b25081_007ea,
        b25081_007m::bigint           AS b25081_007m,
        upper(b25081_007ma::text)     AS b25081_007ma,
        b25081_008e::bigint           AS b25081_008e,
        upper(b25081_008ea::text)     AS b25081_008ea,
        b25081_008m::bigint           AS b25081_008m,
        upper(b25081_008ma::text)     AS b25081_008ma,
        b25081_009e::bigint           AS b25081_009e,
        upper(b25081_009ea::text)     AS b25081_009ea,
        b25081_009m::bigint           AS b25081_009m,
        upper(b25081_009ma::text)     AS b25081_009ma,
        upper(state::text)            AS state,
        upper(county::text)           AS county,
        upper(tract::text)            AS tract,
        upper(dataset_base_url::text) AS dataset_base_url,
        dataset_id::bigint            AS dataset_id,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cc_mortgage_status_by_tract_acs5') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
