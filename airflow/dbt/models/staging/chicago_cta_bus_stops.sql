{{ config(materialized='table') }}

WITH current_pull_with_distinct_combos_numbered AS (
  SELECT *, row_number() over(
    partition by city, systemstop, public_nam, cross_st, pos, owlroutes, dir, street, routesstpg,
                 geometry, source_data_updated, ingestion_check_time
          ) as rn
  FROM {{ source('staging', 'temp_chicago_cta_bus_stops') }}
),
distinct_records_in_current_pull AS (
  SELECT *
  FROM current_pull_with_distinct_combos_numbered
  WHERE rn = 1
),
records_in_data_raw_table AS (
  SELECT *
  FROM {{ source('staging', 'chicago_cta_bus_stops') }}
),
new_or_updated_records_in_current_pull AS (
  SELECT city, systemstop, public_nam, cross_st, pos, owlroutes, dir, street, routesstpg, geometry,
         source_data_updated, ingestion_check_time
  FROM distinct_records_in_current_pull new
  WHERE NOT EXISTS (
    SELECT
    FROM records_in_data_raw_table old
    WHERE AND ((new.city = old.city) OR
               (new.city IS NULL AND old.city IS NULL))
          AND ((new.systemstop = old.systemstop) OR
               (new.systemstop IS NULL AND old.systemstop IS NULL))
          AND ((new.public_nam = old.public_nam) OR
               (new.public_nam IS NULL AND old.public_nam IS NULL))
          AND ((new.cross_st = old.cross_st) OR
               (new.cross_st IS NULL AND old.cross_st IS NULL))
          AND ((new.pos = old.pos) OR
               (new.pos IS NULL AND old.pos IS NULL))
          AND ((new.owlroutes = old.owlroutes) OR
               (new.owlroutes IS NULL AND old.owlroutes IS NULL))
          AND ((new.dir = old.dir) OR
               (new.dir IS NULL AND old.dir IS NULL))
          AND ((new.street = old.street) OR
               (new.street IS NULL AND old.street IS NULL))
          AND ((new.routesstpg = old.routesstpg) OR
               (new.routesstpg IS NULL AND old.routesstpg IS NULL))
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
ORDER BY systemstop
