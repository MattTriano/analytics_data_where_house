{{ config(materialized='view') }}
{% set ck_cols = ["rowid"] %}
{% set record_id = "rowid" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(rowid::text)                      AS rowid,
        upper(appealid::text)                   AS appealid,
        upper(appealtrk::text)                  AS appealtrk,
        upper(appealseq::text)                  AS appealseq,
        pin::bigint                             AS pin,
        upper(pin_dash::text)                   AS pin_dash,
        pin10::bigint                           AS pin10,
        upper(township_code::text)              AS township_code,
        to_date(tax_year::varchar, 'yyyy')      AS tax_year,
        upper(class::text)                      AS class,
        upper(majorclass::text)                 AS majorclass,
        upper(result::text)                     AS result,
        changereason::smallint                  AS changereason,
        upper(changereasondescription::text)    AS changereasondescription,
        nochangereason::smallint                AS nochangereason,
        upper(nochangereasondescription::text)  AS nochangereasondescription,
        bor_landvalue::real::bigint             AS bor_landvalue,
        bor_improvementvalue::real::bigint      AS bor_improvementvalue,
        bor_totalvalue::real::bigint            AS bor_totalvalue,
        assessor_landvalue::real::bigint        AS assessor_landvalue,
        assessor_improvementvalue::real::bigint AS assessor_improvementvalue,
        assessor_totalvalue::real::bigint       AS assessor_totalvalue,
        upper(taxcode::text)                    AS taxcode,
        upper(vol::text)                        AS vol,
        upper(appealtype::text)                 AS appealtype,
        upper(appealtypedescription::text)      AS appealtypedescription,
        upper(appellant::text)                  AS appellant,
        upper(appellant_address::text)          AS appellant_address,
        upper(appellant_city::text)             AS appellant_city,
        upper(appellant_zip::text)              AS appellant_zip,
        upper(appellant_state::text)            AS appellant_state,
        upper(attny::text)                      AS attny,
        upper(attorneycode::text)               AS attorneycode,
        upper(attorney_firmname::text)          AS attorney_firmname,
        upper(attorney_firstname::text)         AS attorney_firstname,
        upper(attorney_lastname::text)          AS attorney_lastname,
        xcoord_crs_3435::double precision       AS xcoord_crs_3435,
        ycoord_crs_3435::double precision       AS ycoord_crs_3435,
        lat::double precision                   AS lat,
        long::double precision                  AS long,
        geometry::GEOMETRY(POINT, 4326)         AS geometry,
        source_data_updated::timestamptz        AS source_data_updated,
        ingestion_check_time::timestamptz       AS ingestion_check_time
    FROM {{ ref('cook_county_parcel_assessment_appeals') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
