{{ config(materialized='table') }}

WITH current_pull_with_distinct_combos_numbered AS (
  SELECT *, row_number() over(
    partition by zip_code, state_house_district, area, incident_fbi_cd, incident_fbi_descr,
                 street_outreach_organization, incident_primary, latitude, day_of_week, unique_id,
                 age, longitude, hour, sex, block, homicide_victim_mi, ward, updated, date,
                 incident_iucr_cd, beat, victimization_iucr_secondary, race, victimization_iucr_cd,
                 victimization_fbi_descr, community_area, location_description, 
                 homicide_victim_last_name, incident_iucr_secondary, district, 
                 victimization_primary, victimization_fbi_cd, case_number, state_senate_district,
                 gunshot_injury_i, month, homicide_victim_first_name, geometry,
                 source_data_updated, ingestion_check_time
          ) as rn
  FROM {{ source('staging','temp_chicago_homicide_and_shooting_victimizations') }}
),
distinct_records_in_current_pull AS (
  SELECT *
  FROM current_pull_with_distinct_combos_numbered
  WHERE rn = 1
),
records_in_data_raw_table AS (
  SELECT *
  FROM {{ source('staging','chicago_homicide_and_shooting_victimizations') }}
),
new_or_updated_records_in_current_pull AS (
  SELECT zip_code, state_house_district, area, incident_fbi_cd, incident_fbi_descr,
         street_outreach_organization, incident_primary, latitude, day_of_week, unique_id, age,
         longitude, hour, sex, block, homicide_victim_mi, ward, updated, date, incident_iucr_cd,
         beat, victimization_iucr_secondary, race, victimization_iucr_cd, victimization_fbi_descr,
        community_area, location_description, homicide_victim_last_name, incident_iucr_secondary,
        district, victimization_primary, victimization_fbi_cd, case_number, state_senate_district,
        gunshot_injury_i, month, homicide_victim_first_name, geometry, source_data_updated,
        ingestion_check_time
  FROM distinct_records_in_current_pull new
  WHERE NOT EXISTS (
    SELECT
    FROM records_in_data_raw_table old
    WHERE ((new.zip_code = old.zip_code) OR
           (new.zip_code IS NULL AND old.zip_code IS NULL))
      AND ((new.state_house_district = old.state_house_district) OR
           (new.state_house_district IS NULL AND old.state_house_district IS NULL))
      AND ((new.area = old.area) OR
           (new.area IS NULL AND old.area IS NULL))
      AND ((new.incident_fbi_cd = old.incident_fbi_cd) OR
           (new.incident_fbi_cd IS NULL AND old.incident_fbi_cd IS NULL))
      AND ((new.incident_fbi_descr = old.incident_fbi_descr) OR
           (new.incident_fbi_descr IS NULL AND old.incident_fbi_descr IS NULL))
      AND ((new.street_outreach_organization = old.street_outreach_organization) OR
           (new.street_outreach_organization IS NULL AND old.street_outreach_organization IS NULL))
      AND ((new.incident_primary = old.incident_primary) OR
           (new.incident_primary IS NULL AND old.incident_primary IS NULL))
      AND ((new.latitude = old.latitude) OR
           (new.latitude IS NULL AND old.latitude IS NULL))
      AND ((new.day_of_week = old.day_of_week) OR
           (new.day_of_week IS NULL AND old.day_of_week IS NULL))
      AND ((new.unique_id = old.unique_id) OR
           (new.unique_id IS NULL AND old.unique_id IS NULL))
      AND ((new.age = old.age) OR
           (new.age IS NULL AND old.age IS NULL))
      AND ((new.longitude = old.longitude) OR
           (new.longitude IS NULL AND old.longitude IS NULL))
      AND ((new.hour = old.hour) OR
           (new.hour IS NULL AND old.hour IS NULL))
      AND ((new.sex = old.sex) OR
           (new.sex IS NULL AND old.sex IS NULL))
      AND ((new.block = old.block) OR
           (new.block IS NULL AND old.block IS NULL))
      AND ((new.homicide_victim_mi = old.homicide_victim_mi) OR
           (new.homicide_victim_mi IS NULL AND old.homicide_victim_mi IS NULL))
      AND ((new.ward = old.ward) OR
           (new.ward IS NULL AND old.ward IS NULL))
      AND ((new.updated = old.updated) OR
           (new.updated IS NULL AND old.updated IS NULL))
      AND ((new.date = old.date) OR
           (new.date IS NULL AND old.date IS NULL))
      AND ((new.incident_iucr_cd = old.incident_iucr_cd) OR
           (new.incident_iucr_cd IS NULL AND old.incident_iucr_cd IS NULL))
      AND ((new.beat = old.beat) OR
           (new.beat IS NULL AND old.beat IS NULL))
      AND ((new.victimization_iucr_secondary = old.victimization_iucr_secondary) OR
           (new.victimization_iucr_secondary IS NULL AND old.victimization_iucr_secondary IS NULL))
      AND ((new.race = old.race) OR
           (new.race IS NULL AND old.race IS NULL))
      AND ((new.victimization_iucr_cd = old.victimization_iucr_cd) OR
           (new.victimization_iucr_cd IS NULL AND old.victimization_iucr_cd IS NULL))
      AND ((new.victimization_fbi_descr = old.victimization_fbi_descr) OR
           (new.victimization_fbi_descr IS NULL AND old.victimization_fbi_descr IS NULL))
      AND ((new.community_area = old.community_area) OR
           (new.community_area IS NULL AND old.community_area IS NULL))
      AND ((new.location_description = old.location_description) OR
           (new.location_description IS NULL AND old.location_description IS NULL))
      AND ((new.homicide_victim_last_name = old.homicide_victim_last_name) OR
           (new.homicide_victim_last_name IS NULL AND old.homicide_victim_last_name IS NULL))
      AND ((new.incident_iucr_secondary = old.incident_iucr_secondary) OR
           (new.incident_iucr_secondary IS NULL AND old.incident_iucr_secondary IS NULL))
      AND ((new.district = old.district) OR
           (new.district IS NULL AND old.district IS NULL))
      AND ((new.victimization_primary = old.victimization_primary) OR
           (new.victimization_primary IS NULL AND old.victimization_primary IS NULL))
      AND ((new.victimization_fbi_cd = old.victimization_fbi_cd) OR
           (new.victimization_fbi_cd IS NULL AND old.victimization_fbi_cd IS NULL))
      AND ((new.case_number = old.case_number) OR
           (new.case_number IS NULL AND old.case_number IS NULL))
      AND ((new.state_senate_district = old.state_senate_district) OR
           (new.state_senate_district IS NULL AND old.state_senate_district IS NULL))
      AND ((new.gunshot_injury_i = old.gunshot_injury_i) OR
           (new.gunshot_injury_i IS NULL AND old.gunshot_injury_i IS NULL))
      AND ((new.month = old.month) OR
           (new.month IS NULL AND old.month IS NULL))
      AND ((new.homicide_victim_first_name = old.homicide_victim_first_name) OR
           (new.homicide_victim_first_name IS NULL AND old.homicide_victim_first_name IS NULL))
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
ORDER BY date, unique_id
