{{ config(materialized='table') }}

WITH current_pull_with_distinct_combos_numbered AS (
  SELECT *, row_number() over(
    partition by pin, property_address, property_apt_no, property_city, property_zip,
                 mailing_address, mailing_state, mailing_city, mailing_zip, longitude, latitude,
                 township, township_name, nbhd, tract_geoid, tract_pop, tract_white_perc,
                 tract_black_perc, tract_asian_perc, tract_his_perc, tract_other_perc,
                 tract_midincome, puma, municipality_fips, municipality, commissioner_dist,
                 reps_dist, senate_dist, ward, ssa_name, ssa_no, tif_agencynum, ohare_noise,
                 floodplain, fs_flood_factor, fs_flood_risk_direction, withinmr100, withinmr101300,
                 school_elem_district, school_hs_district, indicator_has_address,
                 indicator_has_latlon, source_data_updated, ingestion_check_time
          ) as rn
  FROM {{ source('staging','temp_cook_county_parcel_locations') }}
),
distinct_records_in_current_pull AS (
  SELECT *
  FROM current_pull_with_distinct_combos_numbered
  WHERE rn = 1
),
records_in_data_raw_table AS (
  SELECT *
  FROM {{ source('staging','cook_county_parcel_locations') }}
),
new_or_updated_records_in_current_pull AS (
  SELECT pin, property_address, property_apt_no, property_city, property_zip, mailing_address,
         mailing_state, mailing_city, mailing_zip, longitude, latitude, township, township_name,
         nbhd, tract_geoid, tract_pop, tract_white_perc, tract_black_perc, tract_asian_perc,
         tract_his_perc, tract_other_perc, tract_midincome, puma, municipality_fips, municipality,
         commissioner_dist, reps_dist, senate_dist, ward, ssa_name, ssa_no, tif_agencynum,
         ohare_noise, floodplain, fs_flood_factor, fs_flood_risk_direction, withinmr100,
         withinmr101300, school_elem_district, school_hs_district, indicator_has_address,
         indicator_has_latlon, source_data_updated, ingestion_check_time
  FROM distinct_records_in_current_pull new
  WHERE NOT EXISTS (
    SELECT
    FROM records_in_data_raw_table old
    WHERE ((new.pin = old.pin) OR
           (new.pin IS NULL AND old.pin IS NULL))
      AND ((new.property_address = old.property_address) OR
           (new.property_address IS NULL AND old.property_address IS NULL))
      AND ((new.property_apt_no = old.property_apt_no) OR
           (new.property_apt_no IS NULL AND old.property_apt_no IS NULL))
      AND ((new.property_city = old.property_city) OR
           (new.property_city IS NULL AND old.property_city IS NULL))
      AND ((new.property_zip = old.property_zip) OR
           (new.property_zip IS NULL AND old.property_zip IS NULL))
      AND ((new.mailing_address = old.mailing_address) OR
           (new.mailing_address IS NULL AND old.mailing_address IS NULL))
      AND ((new.mailing_state = old.mailing_state) OR
           (new.mailing_state IS NULL AND old.mailing_state IS NULL))
      AND ((new.mailing_city = old.mailing_city) OR
           (new.mailing_city IS NULL AND old.mailing_city IS NULL))
      AND ((new.mailing_zip = old.mailing_zip) OR
           (new.mailing_zip IS NULL AND old.mailing_zip IS NULL))
      AND ((new.longitude = old.longitude) OR
           (new.longitude IS NULL AND old.longitude IS NULL))
      AND ((new.latitude = old.latitude) OR
           (new.latitude IS NULL AND old.latitude IS NULL))
      AND ((new.township = old.township) OR
           (new.township IS NULL AND old.township IS NULL))
      AND ((new.township_name = old.township_name) OR
           (new.township_name IS NULL AND old.township_name IS NULL))
      AND ((new.nbhd = old.nbhd) OR
           (new.nbhd IS NULL AND old.nbhd IS NULL))
      AND ((new.tract_geoid = old.tract_geoid) OR
           (new.tract_geoid IS NULL AND old.tract_geoid IS NULL))
      AND ((new.tract_pop = old.tract_pop) OR
           (new.tract_pop IS NULL AND old.tract_pop IS NULL))
      AND ((new.tract_white_perc = old.tract_white_perc) OR
           (new.tract_white_perc IS NULL AND old.tract_white_perc IS NULL))
      AND ((new.tract_black_perc = old.tract_black_perc) OR
           (new.tract_black_perc IS NULL AND old.tract_black_perc IS NULL))
      AND ((new.tract_asian_perc = old.tract_asian_perc) OR
           (new.tract_asian_perc IS NULL AND old.tract_asian_perc IS NULL))
      AND ((new.tract_his_perc = old.tract_his_perc) OR
           (new.tract_his_perc IS NULL AND old.tract_his_perc IS NULL))
      AND ((new.tract_other_perc = old.tract_other_perc) OR
           (new.tract_other_perc IS NULL AND old.tract_other_perc IS NULL))
      AND ((new.tract_midincome = old.tract_midincome) OR
           (new.tract_midincome IS NULL AND old.tract_midincome IS NULL))
      AND ((new.puma = old.puma) OR
           (new.puma IS NULL AND old.puma IS NULL))
      AND ((new.municipality_fips = old.municipality_fips) OR
           (new.municipality_fips IS NULL AND old.municipality_fips IS NULL))
      AND ((new.municipality = old.municipality) OR
           (new.municipality IS NULL AND old.municipality IS NULL))
      AND ((new.commissioner_dist = old.commissioner_dist) OR
           (new.commissioner_dist IS NULL AND old.commissioner_dist IS NULL))
      AND ((new.reps_dist = old.reps_dist) OR
           (new.reps_dist IS NULL AND old.reps_dist IS NULL))
      AND ((new.senate_dist = old.senate_dist) OR
           (new.senate_dist IS NULL AND old.senate_dist IS NULL))
      AND ((new.ward = old.ward) OR
           (new.ward IS NULL AND old.ward IS NULL))
      AND ((new.ssa_name = old.ssa_name) OR
           (new.ssa_name IS NULL AND old.ssa_name IS NULL))
      AND ((new.ssa_no = old.ssa_no) OR
           (new.ssa_no IS NULL AND old.ssa_no IS NULL))
      AND ((new.tif_agencynum = old.tif_agencynum) OR
           (new.tif_agencynum IS NULL AND old.tif_agencynum IS NULL))
      AND ((new.ohare_noise = old.ohare_noise) OR
           (new.ohare_noise IS NULL AND old.ohare_noise IS NULL))
      AND ((new.floodplain = old.floodplain) OR
           (new.floodplain IS NULL AND old.floodplain IS NULL))
      AND ((new.fs_flood_factor = old.fs_flood_factor) OR
           (new.fs_flood_factor IS NULL AND old.fs_flood_factor IS NULL))
      AND ((new.fs_flood_risk_direction = old.fs_flood_risk_direction) OR
           (new.fs_flood_risk_direction IS NULL AND old.fs_flood_risk_direction IS NULL))
      AND ((new.withinmr100 = old.withinmr100) OR
           (new.withinmr100 IS NULL AND old.withinmr100 IS NULL))
      AND ((new.withinmr101300 = old.withinmr101300) OR
           (new.withinmr101300 IS NULL AND old.withinmr101300 IS NULL))
      AND ((new.school_elem_district = old.school_elem_district) OR
           (new.school_elem_district IS NULL AND old.school_elem_district IS NULL))
      AND ((new.school_hs_district = old.school_hs_district) OR
           (new.school_hs_district IS NULL AND old.school_hs_district IS NULL))
      AND ((new.indicator_has_address = old.indicator_has_address) OR
           (new.indicator_has_address IS NULL AND old.indicator_has_address IS NULL))
      AND ((new.indicator_has_latlon = old.indicator_has_latlon) OR
           (new.indicator_has_latlon IS NULL AND old.indicator_has_latlon IS NULL))
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
ORDER BY pin
