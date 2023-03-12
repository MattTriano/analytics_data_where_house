{{ config(materialized='table') }}

WITH parcel_assessments AS (
    SELECT
        parcel_assessment_id,
        pin,
        tax_year,
        class,
        LAG(class, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC) AS class_last_assessment,
        township_code,
        township_name,
        mailed_bldg,
        mailed_land,
        mailed_tot,
        certified_bldg,
        certified_land,
        certified_tot,
        board_bldg,
        board_land,
        board_tot
    FROM {{ ref('cook_county_parcel_value_assessments_clean') }}
)

SELECT *
FROM parcel_assessments