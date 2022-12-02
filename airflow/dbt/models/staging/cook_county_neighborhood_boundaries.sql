{{ config(materialized='view') }}

WITH data_raw_neighborhood_boundary_data AS (
  SELECT *
  FROM {{ source('staging','cook_county_neighborhood_boundaries') }}
),
temp_neighborhood_boundary_data AS (
  SELECT * 
  FROM {{ ref('stg_cc_neighborhoods') }} stg
  WHERE NOT EXISTS (
    SELECT
    FROM data_raw_neighborhood_boundary_data raw
    WHERE stg.triad_name = raw.triad_name AND
          stg.town_nbhd = raw.town_nbhd AND
          stg.township_code = raw.township_code AND
          stg.triad_code = raw.triad_code AND
          stg.township_name = raw.township_name AND
          stg.nbhd = raw.nbhd AND
          stg.geometry = raw.geometry AND
          stg.source_data_updated = raw.source_data_updated AND
          stg.ingestion_check_time = raw.ingestion_check_time
  )
),
new_data_raw_neighborhood_boundary_data AS (
  SELECT * 
  FROM data_raw_neighborhood_boundary_data
    UNION ALL
  SELECT *
  FROM temp_neighborhood_boundary_data
)

SELECT * 
FROM new_data_raw_neighborhood_boundary_data
