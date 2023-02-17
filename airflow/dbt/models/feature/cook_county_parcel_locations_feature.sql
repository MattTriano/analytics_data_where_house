{{ config(materialized='table') }}

WITH parcel_loc_features AS (
    SELECT
        parcel_location_id,
        pin,
        property_city,
        CONCAT(township, nbhd)                              AS town_nbhd,
        CASE
            WHEN property_address = mailing_address THEN true
            WHEN 
                property_address <> mailing_address
                AND property_address IS NOT NULL
                AND mailing_address IS NOT NULL THEN false
            ELSE NULL
        END                                                 AS owner_occupied,
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geometry
    FROM {{ ref('cook_county_parcel_locations_clean') }}
),
parcel_buyers AS (
    SELECT 
        pin, 
        sale_buyer_name,
        row_number() over(PARTITION BY pin ORDER BY pin, sale_date DESC) AS rn
    FROM {{ ref('cook_county_parcel_sales_clean') }}
),
latest_buyer AS (
    SELECT 
        pin,
        sale_buyer_name AS owner_name
    FROM parcel_buyers
    WHERE rn = 1
),
chicago_pins AS (
    SELECT
        pin,
        geometry
    FROM parcel_loc_features
    WHERE property_city = 'CHICAGO'
),
chicago_areas AS (
    SELECT
        loc.pin,
        loc.geometry,
        pdd.dist_num AS cpd_district,
        pdb.beat_num AS cpd_beat,
        cca.community AS chicago_community
    FROM chicago_pins AS loc
    LEFT JOIN {{ ref('chicago_police_district_boundaries_clean') }} AS pdd
    ON ST_Contains(pdd.geometry, loc.geometry)
    LEFT JOIN {{ ref('chicago_police_beat_boundaries_clean') }} AS pdb
    ON ST_Contains(pdb.geometry, loc.geometry)
    LEFT JOIN {{ ref('chicago_community_area_boundaries_clean') }} AS cca
    ON ST_Contains(cca.geometry, loc.geometry)
)

SELECT
    lf.parcel_location_id,
    lf.pin,
    lf.town_nbhd,
    lf.owner_occupied,
    lb.owner_name,
    ca.cpd_beat,
    ca.cpd_district,
    ca.chicago_community,
    lf.geometry
FROM parcel_loc_features AS lf
INNER JOIN latest_buyer AS lb
ON lf.pin = lb.pin
LEFT JOIN chicago_areas AS ca
ON lf.pin = ca.pin
ORDER BY lf.pin
