{{ config(materialized='view') }}
{% set ck_cols = ["pin", "tax_year"] %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS parcel_assessment_id,
        pin::bigint AS pin,
        tax_year::int AS tax_year,
        upper(class::text) AS class,
        lpad(township_code::int::varchar(2), 2, '0') AS township_code,
        upper(township_name::text) AS township_name,
        mailed_bldg::double precision AS mailed_bldg,
        mailed_land::double precision AS mailed_land,
        mailed_tot::double precision AS mailed_tot,
        certified_bldg::double precision AS certified_bldg,
        certified_land::double precision AS certified_land,
        certified_tot::double precision AS certified_tot,
        board_bldg::double precision AS board_bldg,
        board_land::double precision AS board_land,
        board_tot::double precision AS board_tot,
        source_data_updated::timestamptz AS source_data_updated,
        ingestion_check_time::timestamptz AS ingestion_check_time
    FROM {{ ref('cook_county_parcel_value_assessments') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated