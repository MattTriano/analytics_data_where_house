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
    WHERE new.city = old.city AND
          new.systemstop = old.systemstop AND
          new.public_nam = old.public_nam AND
          new.cross_st = old.cross_st AND
          new.pos = old.pos AND
          new.owlroutes = old.owlroutes AND
          new.dir = old.dir AND
          new.street = old.street AND
          new.routesstpg = old.routesstpg AND
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
ORDER BY systemstop
