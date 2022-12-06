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
    WHERE ((new.location_state = old.location_state) OR
           (new.location_state IS NULL AND old.location_state IS NULL))
      AND ((new.location_zip = old.location_zip) OR
           (new.location_zip IS NULL AND old.location_zip IS NULL))
      AND ((new.station_descriptive_name = old.station_descriptive_name) OR
           (new.station_descriptive_name IS NULL AND old.station_descriptive_name IS NULL))
      AND ((new.blue = old.blue) OR
           (new.blue IS NULL AND old.blue IS NULL))
      AND ((new.station_name = old.station_name) OR
           (new.station_name IS NULL AND old.station_name IS NULL))
      AND ((new.y = old.y) OR
           (new.y IS NULL AND old.y IS NULL))
      AND ((new.location_address = old.location_address) OR
           (new.location_address IS NULL AND old.location_address IS NULL))
      AND ((new.location_city = old.location_city) OR
           (new.location_city IS NULL AND old.location_city IS NULL))
      AND ((new.brn = old.brn) OR
           (new.brn IS NULL AND old.brn IS NULL))
      AND ((new.direction_id = old.direction_id) OR
           (new.direction_id IS NULL AND old.direction_id IS NULL))
      AND ((new.map_id = old.map_id) OR
           (new.map_id IS NULL AND old.map_id IS NULL))
      AND ((new.g = old.g) OR
           (new.g IS NULL AND old.g IS NULL))
      AND ((new.stop_name = old.stop_name) OR
           (new.stop_name IS NULL AND old.stop_name IS NULL))
      AND ((new.p = old.p) OR
           (new.p IS NULL AND old.p IS NULL))
      AND ((new.ada = old.ada) OR
           (new.ada IS NULL AND old.ada IS NULL))
      AND ((new.pnk = old.pnk) OR
           (new.pnk IS NULL AND old.pnk IS NULL))
      AND ((new.pexp = old.pexp) OR
           (new.pexp IS NULL AND old.pexp IS NULL))
      AND ((new.stop_id = old.stop_id) OR
           (new.stop_id IS NULL AND old.stop_id IS NULL))
      AND ((new.red = old.red) OR
           (new.red IS NULL AND old.red IS NULL))
      AND ((new.o = old.o) OR
           (new.o IS NULL AND old.o IS NULL))
      AND ((new.geometry = old.geometry) OR
           (new.geometry IS NULL AND old.geometry IS NULL))
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
