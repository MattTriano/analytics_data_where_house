{{ config(materialized='table') }}
{% set source_cols = [
     "zip_code", "state_house_district", "area", "incident_fbi_cd", "incident_fbi_descr",
     "street_outreach_organization", "incident_primary", "latitude", "day_of_week", "unique_id",
     "age", "longitude", "hour", "sex", "block", "homicide_victim_mi", "ward", "updated", "date",
     "incident_iucr_cd", "beat", "victimization_iucr_secondary", "race", "victimization_iucr_cd",
     "victimization_fbi_descr", "community_area", "location_description",
     "homicide_victim_last_name", "incident_iucr_secondary", "district", "victimization_primary",
     "victimization_fbi_cd", "case_number", "state_senate_district", "gunshot_injury_i", "month",
     "homicide_victim_first_name", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
     SELECT *, 1 AS retention_priority
     FROM {{ source('staging', 'chicago_homicide_and_shooting_victimizations') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
     SELECT *,
          row_number() over(partition by
               {% for sc in source_cols %}{{ sc }},{% endfor %}
               {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
          ) as rn
     FROM {{ source('staging', 'temp_chicago_homicide_and_shooting_victimizations') }}
),
distinct_records_in_current_pull AS (
     SELECT
          {% for sc in source_cols %}{{ sc }},{% endfor %}
          {% for mc in metadata_cols %}{{ mc }},{% endfor %}
          2 AS retention_priority
     FROM current_pull_with_distinct_combos_numbered
     WHERE rn = 1
),

-- stacking the existing data with all distinct records from the latest pull
data_raw_table_with_all_new_and_updated_records AS (
     SELECT *
     FROM records_in_data_raw_table
          UNION ALL
     SELECT *
     FROM distinct_records_in_current_pull
),

-- selecting records that where source columns are distinct (keeping the earlier recovery
--  when there are duplicates to chose from)
data_raw_table_with_new_and_updated_records AS (
     SELECT *,
      row_number() over(partition by
          {% for sc in source_cols %}{{ sc }}{{ "," if not loop.last }}{% endfor %}
          ORDER BY retention_priority
          ) as rn
     FROM data_raw_table_with_all_new_and_updated_records
),
distinct_records_for_data_raw_table AS (
     SELECT
          {% for sc in source_cols %}{{ sc }},{% endfor %}
          {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
     FROM data_raw_table_with_new_and_updated_records
     WHERE rn = 1
)

SELECT *
FROM distinct_records_for_data_raw_table
ORDER BY unique_id, source_data_updated
