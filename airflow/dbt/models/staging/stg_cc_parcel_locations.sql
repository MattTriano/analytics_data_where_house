{{ config(materialized='view') }}

WITH parcel_location_data AS
(
  SELECT *, row_number() over(partition by pin) as rn
  FROM {{ source('staging','temp_cook_county_parcel_locations') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['pin']) }} AS parcel_loc_id,
    pin::bigint AS pin,
    upper(property_address::text) AS property_address,
    upper(property_apt_no::text) AS property_apt_no,
    upper(property_city::text) AS property_city,
    upper(property_zip::varchar(10)) AS property_zip,
    upper(mailing_address::text) AS mailing_address,
    upper(mailing_state::char(2)) AS mailing_state,
    upper(mailing_city::text) AS mailing_city,
    mailing_zip::int::varchar(10) AS mailing_zip,
    longitude::double precision AS longitude,
    latitude::double precision AS latitude,
    lpad(township::int::varchar, 2, '0') AS township,
    upper(township_name::text) AS township_name,
    lpad(nbhd::int::varchar(3), 3, '0') AS nbhd,
    lpad(tract_geoid::bigint::varchar(11), 11, '0') AS tract_geoid,
    tract_pop::int AS tract_pop,
    tract_white_perc::double precision AS tract_white_perc,
    tract_black_perc::double precision AS tract_black_perc,
    tract_asian_perc::double precision AS tract_asian_perc,
    tract_his_perc::double precision AS tract_his_perc,
    tract_other_perc::double precision AS tract_other_perc,
    tract_midincome::int AS tract_midincome,
    lpad(puma::int::varchar(4), 4, '0') AS puma,
    lpad(municipality_fips::int::varchar(4), 4, '0') AS municipality_fips,
    upper(municipality::text) AS municipality,
    upper(commissioner_dist::varchar(5)) AS commissioner_dist,
    reps_dist::int AS reps_dist,
    senate_dist::int AS senate_dist,
    ward::int AS ward,
    upper(ssa_name::text) AS ssa_name,
    upper(ssa_no::text) AS ssa_no,
    lpad(tif_agencynum::int::varchar(8), 8, '0') AS tif_agencynum,
    ohare_noise::int::boolean AS ohare_noise,
    floodplain::int::boolean AS floodplain,
    fs_flood_factor::int::boolean AS fs_flood_factor,
    fs_flood_risk_direction::int AS fs_flood_risk_direction,
    withinmr100::int::boolean AS withinmr100,
    withinmr101300::int::boolean AS withinmr101300,
    upper(school_elem_district::text) AS school_elem_district,
    upper(school_hs_district::text) AS school_hs_district,
    indicator_has_address::int::boolean AS indicator_has_address,
    indicator_has_latlon::int::boolean AS indicator_has_latlon,
    ingested_on::text AS ingested_on
FROM parcel_location_data
WHERE rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
