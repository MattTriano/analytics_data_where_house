{{ config(materialized='view') }}

WITH parcel_loc_features AS (
    SELECT
        parcel_location_id,
        pin,
        CASE
            WHEN property_address = mailing_address THEN true
            WHEN 
                property_address <> mailing_address
                AND property_address IS NOT NULL
                AND mailing_address IS NOT NULL THEN false
            ELSE NULL
        END CASE AS owner_occupied,
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
)

SELECT *
FROM parcel_loc_features
ORDER BY pin, sale_date
