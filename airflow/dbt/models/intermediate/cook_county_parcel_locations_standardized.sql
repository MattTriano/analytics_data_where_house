{{ config(materialized='view') }}
{% set ck_cols = ["pin"] %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }}   AS parcel_location_id,
        pin::bigint                                       AS pin,
        upper(property_address::text)                     AS property_address,
        upper(property_apt_no::text)                      AS property_apt_no,
        upper(property_city::text)                        AS property_city,
        upper(property_zip::text)                         AS property_zip,
        lpad(nbhd::int::varchar(3), 3, '0')               AS nbhd,
        lpad(township::int::varchar, 2, '0')              AS township,
        upper(township_name::text)                        AS township_name,
        upper(municipality::text)                         AS municipality,
        lpad(municipality_fips::int::varchar(4), 4, '0')  AS municipality_fips,
        ward::int                                         AS ward,
        lpad(puma::int::varchar(4), 4, '0')               AS puma,
        lpad(tract_geoid::bigint::varchar(11), 11, '0')   AS tract_geoid,
        tract_pop::int                                    AS tract_pop,
        tract_white_perc::double precision                AS tract_white_perc,
        tract_black_perc::double precision                AS tract_black_perc,
        tract_asian_perc::double precision                AS tract_asian_perc,
        tract_his_perc::double precision                  AS tract_his_perc,
        tract_other_perc::double precision                AS tract_other_perc,
        tract_midincome::int                              AS tract_midincome,
        upper(commissioner_dist::varchar(5))              AS commissioner_dist,
        reps_dist::int                                    AS reps_dist,
        senate_dist::int                                  AS senate_dist,
        upper(ssa_name::text)                             AS ssa_name,
        upper(ssa_no::text)                               AS ssa_no,
        lpad(tif_agencynum::int::varchar(8), 8, '0')      AS tif_agencynum,
        upper(school_elem_district::text)                 AS school_elem_district,
        upper(school_hs_district::text)                   AS school_hs_district,
        upper(mailing_address::text)                      AS mailing_address,
        upper(mailing_city::text)                         AS mailing_city,
        lpad(upper(mailing_zip::char(5)), 5, '0')         AS mailing_zip,
        upper(mailing_state::text)                        AS mailing_state,
        ohare_noise::int::boolean                         AS ohare_noise,
        floodplain::int::boolean                          AS floodplain,
        fs_flood_factor::int::boolean                     AS fs_flood_factor,
        fs_flood_risk_direction::int                      AS fs_flood_risk_direction,
        withinmr100::int::boolean                         AS withinmr100,
        withinmr101300::int::boolean                      AS withinmr101300,
        indicator_has_address::int::boolean               AS indicator_has_address,
        indicator_has_latlon::int::boolean                AS indicator_has_latlon,
        longitude::double precision                       AS longitude,
        latitude::double precision                        AS latitude,
        ST_SetSRID(ST_Point( longitude, latitude), 4326)  AS geometry,
        source_data_updated::timestamptz                  AS source_data_updated,
        ingestion_check_time::timestamptz                 AS ingestion_check_time
    FROM {{ ref('cook_county_parcel_locations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
