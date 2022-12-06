{{ config(materialized='table') }}

WITH current_pull_with_distinct_combos_numbered AS (
  SELECT *, row_number() over(
    partition by location_state, location_zip, station_descriptive_name, blue, station_name, y,
                 location_address, location_city, brn, direction_id, map_id, g, stop_name, p, ada,
                 pnk, pexp, stop_id, red, o, geometry, source_data_updated, ingestion_check_time
          ) as rn
  FROM {{ source('staging', 'temp_chicago_cta_train_stations') }}
),
distinct_records_in_current_pull AS (
  SELECT *
  FROM current_pull_with_distinct_combos_numbered
  WHERE rn = 1
),
records_in_data_raw_table AS (
  SELECT *
  FROM {{ source('staging', 'chicago_cta_train_stations') }}
),
new_or_updated_records_in_current_pull AS (
  SELECT location_state, location_zip, station_descriptive_name, blue, station_name, y,
         location_address, location_city, brn, direction_id, map_id, g, stop_name, p, ada, pnk,
         pexp, stop_id, red, o, geometry, source_data_updated, ingestion_check_time
  FROM distinct_records_in_current_pull new
  WHERE NOT EXISTS (
    SELECT
    FROM records_in_data_raw_table old
    WHERE new.location_state = old.location_state AND
          new.location_zip = old.location_zip AND
          new.station_descriptive_name = old.station_descriptive_name AND
          new.blue = old.blue AND
          new.station_name = old.station_name AND
          new.y = old.y AND
          new.location_address = old.location_address AND
          new.location_city = old.location_city AND
          new.brn = old.brn AND
          new.direction_id = old.direction_id AND
          new.map_id = old.map_id AND
          new.g = old.g AND
          new.stop_name = old.stop_name AND
          new.p = old.p AND
          new.ada = old.ada AND
          new.pnk = old.pnk AND
          new.pexp = old.pexp AND
          new.stop_id = old.stop_id AND
          new.red = old.red AND
          new.o = old.o AND
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
ORDER BY stop_id
