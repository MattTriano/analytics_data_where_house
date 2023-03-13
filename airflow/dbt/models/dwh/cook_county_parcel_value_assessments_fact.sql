{{ config(materialized='table') }}

WITH parcel_assessments AS (
    SELECT
        parcel_assessment_id,
        pin,
        to_date(tax_year::varchar, 'yyyy') AS tax_year,
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
        board_tot,
        LAG(mailed_bldg, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC)    AS last_mailed_bldg,
        LAG(mailed_land, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC)    AS last_mailed_land,
        LAG(mailed_tot, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC)     AS last_mailed_tot,        
        LAG(certified_bldg, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC) AS last_certified_bldg,
        LAG(certified_land, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC) AS last_certified_land,
        LAG(certified_tot, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC)  AS last_certified_tot,
        LAG(board_bldg, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC)     AS last_board_bldg,
        LAG(board_land, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC)     AS last_board_land,
        LAG(board_tot, 1) OVER (PARTITION BY pin ORDER BY pin ASC, tax_year ASC)      AS last_board_tot
    FROM {{ ref('cook_county_parcel_value_assessments_clean') }}
),
assessments_w_changes AS (
    SELECT
        parcel_assessment_id,
        pin,
        tax_year,
        class,
        class_last_assessment,
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
        board_tot,
        mailed_bldg - last_mailed_bldg       AS mailed_bldg_change,
        mailed_land - last_mailed_land       AS mailed_land_change,
        mailed_tot - last_mailed_tot         AS mailed_tot_change,
        certified_bldg - last_certified_bldg AS certified_bldg_change,
        certified_land - last_certified_land AS certified_land_change,
        certified_tot - last_certified_tot   AS certified_tot_change,
        board_bldg - last_board_bldg         AS board_bldg_change,
        board_land - last_board_land         AS board_land_change,
        board_tot - last_board_tot           AS board_tot_change
    FROM parcel_assessments
)

SELECT *
FROM assessments_w_changes
