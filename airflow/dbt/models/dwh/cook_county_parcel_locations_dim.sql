{{ config(materialized='table') }}

WITH clean_cols AS (
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
        latitude
    FROM {{ ref('cook_county_parcel_locations_clean') }}
    ORDER BY pin
),
feature_cols AS (
    SELECT
        parcel_location_id,
        pin,
        town_nbhd,
        owner_occupied,
        owner_name,
        cpd_beat,
        cpd_district,
        chicago_community,
        geometry
    FROM {{ ref('cook_county_parcel_locations_feature') }}
)

SELECT
    cc.parcel_location_id,
    cc.pin,
    fc.town_nbhd,
    cc.property_address,
    cc.property_apt_no,
    cc.property_city,
    cc.property_zip,
    fc.owner_occupied,
    fc.owner_name,
    fc.cpd_beat,
    fc.cpd_district,
    fc.chicago_community,
    cc.nbhd,
    cc.township,
    cc.township_name,
    cc.municipality,
    cc.municipality_fips,
    cc.ward,
    cc.tract_geoid,
    cc.ohare_noise,
    cc.floodplain,
    cc.fs_flood_factor,
    cc.fs_flood_risk_direction,
    cc.withinmr100,
    cc.withinmr101300,
    cc.longitude,
    cc.latitude,
    fc.geometry
FROM clean_cols AS cc
INNER JOIN feature_cols AS fc
ON cc.parcel_location_id = fc.parcel_location_id
