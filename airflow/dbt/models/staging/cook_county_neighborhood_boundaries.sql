{{ config(materialized='table') }}

WITH current_pull_with_distinct_combos_numbered AS (
  SELECT *, row_number() over(
    partition by triad_name, town_nbhd, township_code, triad_code, township_name, nbhd, geometry,
                 source_data_updated, ingestion_check_time
    ) as rn
  FROM {{ source('staging', 'temp_cook_county_neighborhood_boundaries') }}
),
distinct_records_in_current_pull AS (
  SELECT *
  FROM current_pull_with_distinct_combos_numbered
  WHERE rn = 1
),
records_in_data_raw_table AS (
  SELECT *
  FROM {{ source('staging', 'cook_county_neighborhood_boundaries') }}
),
new_or_updated_records_in_current_pull AS (
  SELECT triad_name, town_nbhd, township_code, triad_code, township_name, nbhd, geometry,
         source_data_updated, ingestion_check_time
  FROM distinct_records_in_current_pull new
  WHERE NOT EXISTS (
    SELECT
    FROM records_in_data_raw_table old
    WHERE new.triad_name = old.triad_name AND
          new.town_nbhd = old.town_nbhd AND
          new.township_code = old.township_code AND
          new.triad_code = old.triad_code AND
          new.township_name = old.township_name AND
          new.nbhd = old.nbhd AND
          new.geometry = old.geometry
  )
),
data_raw_table_with_new_and_updated_records AS (
  SELECT * 
  FROM records_in_data_raw_table
    UNION ALL
  SELECT *
  FROM new_or_updated_records_in_current_pull
)

SELECT * 
FROM data_raw_table_with_new_and_updated_records
ORDER BY town_nbhd, triad_code
