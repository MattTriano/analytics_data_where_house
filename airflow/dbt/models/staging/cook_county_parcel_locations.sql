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
    WHERE new.zip_code = old.zip_code AND
          new.state_house_district = old.state_house_district AND
          new.area = old.area AND
          new.incident_fbi_cd = old.incident_fbi_cd AND
          new.incident_fbi_descr = old.incident_fbi_descr AND
          new.street_outreach_organization = old.street_outreach_organization AND
          new.incident_primary = old.incident_primary AND
          new.latitude = old.latitude AND
          new.day_of_week = old.day_of_week AND
          new.unique_id = old.unique_id AND
          new.age = old.age AND
          new.longitude = old.longitude AND
          new.hour = old.hour AND
          new.sex = old.sex AND
          new.block = old.block AND
          new.homicide_victim_mi = old.homicide_victim_mi AND
          new.ward = old.ward AND
          new.updated = old.updated AND
          new.date = old.date AND
          new.incident_iucr_cd = old.incident_iucr_cd AND
          new.beat = old.beat AND
          new.victimization_iucr_secondary = old.victimization_iucr_secondary AND
          new.race = old.race AND
          new.victimization_iucr_cd = old.victimization_iucr_cd AND
          new.victimization_fbi_descr = old.victimization_fbi_descr AND
          new.community_area = old.community_area AND
          new.location_description = old.location_description AND
          new.homicide_victim_last_name = old.homicide_victim_last_name AND
          new.incident_iucr_secondary = old.incident_iucr_secondary AND
          new.district = old.district AND
          new.victimization_primary = old.victimization_primary AND
          new.victimization_fbi_cd = old.victimization_fbi_cd AND
          new.case_number = old.case_number AND
          new.state_senate_district = old.state_senate_district AND
          new.gunshot_injury_i = old.gunshot_injury_i AND
          new.month = old.month AND
          new.homicide_victim_first_name = old.homicide_victim_first_name AND
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
ORDER BY pin
