{{ config(materialized='view') }}

WITH parcel_loc_features AS (
    SELECT
        parcel_location_id,
        pin,
        CONCAT(township, nbhd) AS town_nbhd,
        CASE
            WHEN property_address = mailing_address THEN true
            WHEN 
                property_address <> mailing_address
                AND property_address IS NOT NULL
                AND mailing_address IS NOT NULL THEN false
            ELSE NULL
        END CASE AS owner_occupied
    FROM {{ ref('cook_county_parcel_locations_clean') }}
),
parcel_buyers AS (
    SELECT 
        pin, 
        sale_buyer_name,
        row_number() over(
			   PARTITION BY pin ORDER BY pin, sale_date DESC
		   ) as rn
    FROM {{ ref('cook_county_parcel_sales_clean') }}
),
latest_buyer AS (
    SELECT 
        pin,
        sale_buyer_name AS owner_name
    FROM latest_buyer
    WHERE rn = 1
)

SELECT
    lf.parcel_location_id,
    lf.pin,
    lf.town_nbhd,
    lf.owner_occupied,
    lb.owner_name
FROM parcel_loc_features AS lf
INNER JOIN latest_buyer AS lb
ON lf.pin = lb.pin
ORDER BY lf.pin,
