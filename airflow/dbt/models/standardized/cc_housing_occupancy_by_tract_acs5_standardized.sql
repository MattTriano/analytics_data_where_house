{{ config(materialized='view') }}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(geo_id::text)           AS geo_id,
        upper(name::text)             AS name,
        b25001_001e::bigint           AS b25001_001e,
        upper(b25001_001ea::text)     AS b25001_001ea,
        b25001_001m::bigint           AS b25001_001m,
        upper(b25001_001ma::text)     AS b25001_001ma,
        b25002_001e::bigint           AS b25002_001e,
        upper(b25002_001ea::text)     AS b25002_001ea,
        b25002_001m::bigint           AS b25002_001m,
        upper(b25002_001ma::text)     AS b25002_001ma,
        b25002_002e::bigint           AS b25002_002e,
        upper(b25002_002ea::text)     AS b25002_002ea,
        b25002_002m::bigint           AS b25002_002m,
        upper(b25002_002ma::text)     AS b25002_002ma,
        b25002_003e::bigint           AS b25002_003e,
        upper(b25002_003ea::text)     AS b25002_003ea,
        b25002_003m::bigint           AS b25002_003m,
        upper(b25002_003ma::text)     AS b25002_003ma,
        b25004_001e::bigint           AS b25004_001e,
        upper(b25004_001ea::text)     AS b25004_001ea,
        b25004_001m::bigint           AS b25004_001m,
        upper(b25004_001ma::text)     AS b25004_001ma,
        b25004_002e::bigint           AS b25004_002e,
        upper(b25004_002ea::text)     AS b25004_002ea,
        b25004_002m::bigint           AS b25004_002m,
        upper(b25004_002ma::text)     AS b25004_002ma,
        b25004_003e::bigint           AS b25004_003e,
        upper(b25004_003ea::text)     AS b25004_003ea,
        b25004_003m::bigint           AS b25004_003m,
        upper(b25004_003ma::text)     AS b25004_003ma,
        b25004_004e::bigint           AS b25004_004e,
        upper(b25004_004ea::text)     AS b25004_004ea,
        b25004_004m::bigint           AS b25004_004m,
        upper(b25004_004ma::text)     AS b25004_004ma,
        b25004_005e::bigint           AS b25004_005e,
        upper(b25004_005ea::text)     AS b25004_005ea,
        b25004_005m::bigint           AS b25004_005m,
        upper(b25004_005ma::text)     AS b25004_005ma,
        b25004_006e::bigint           AS b25004_006e,
        upper(b25004_006ea::text)     AS b25004_006ea,
        b25004_006m::bigint           AS b25004_006m,
        upper(b25004_006ma::text)     AS b25004_006ma,
        b25004_007e::bigint           AS b25004_007e,
        upper(b25004_007ea::text)     AS b25004_007ea,
        b25004_007m::bigint           AS b25004_007m,
        upper(b25004_007ma::text)     AS b25004_007ma,
        b25004_008e::bigint           AS b25004_008e,
        upper(b25004_008ea::text)     AS b25004_008ea,
        b25004_008m::bigint           AS b25004_008m,
        upper(b25004_008ma::text)     AS b25004_008ma,
        upper(state::text)            AS state,
        upper(county::text)           AS county,
        upper(tract::text)            AS tract,
        upper(dataset_base_url::text) AS dataset_base_url,
        dataset_id::bigint            AS dataset_id,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cc_housing_occupancy_by_tract_acs5') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
