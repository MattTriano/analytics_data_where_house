{{ config(materialized='table') }}

SELECT
    parcel_location_id,
    pin,
    property_address,
    property_apt_no,
    property_city,
    property_zip,
    nbhd,
    township,
    township_name,
    CONCAT(township, nbhd),
    municipality,
    municipality_fips,
    ward,
    tract_geoid,
    ohare_noise,
    floodplain,
    fs_flood_factor,
    fs_flood_risk_direction,
    withinmr100,
    withinmr101300,
    longitude,
    latitude,
    geometry
FROM {{ ref('cook_county_parcel_locations_clean') }}
ORDER BY pin
