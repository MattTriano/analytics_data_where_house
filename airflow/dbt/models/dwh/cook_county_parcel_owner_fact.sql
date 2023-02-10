{{ config(materialized='table') }}

WITH owner_features AS (
    SELECT
        parcel_location_id,
        pin,
        town_nbhd,
        owner_occupied,
        owner_name
    FROM {{ ref('cook_county_parcel_locations_feature') }}
)

SELECT 
    of.parcel_location_id,
    of.pin,
    of.owner_occupied,
    of.mailing_address,
    of.mailing_city,
    of.mailing_zip,
    of.mailing_state,
    pl.mailing_address,
    pl.mailing_city,
    pl.mailing_zip,
    pl.mailing_state,
FROM owner_features
INNER JOIN {{ ref('cook_county_parcel_locations_clean') }} AS pl
ON of.parcel_location_id = pl.parcel_location_id