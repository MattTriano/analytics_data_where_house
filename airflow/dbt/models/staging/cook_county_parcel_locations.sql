{{ config(materialized='table') }}
{% set source_cols = [
     "pin", "property_address", "property_apt_no", "property_city", "property_zip",
     "mailing_address", "mailing_state", "mailing_city", "mailing_zip", "longitude", "latitude",
     "township", "township_name", "nbhd", "tract_geoid", "tract_pop", "tract_white_perc",
     "tract_black_perc", "tract_asian_perc", "tract_his_perc", "tract_other_perc",
     "tract_midincome", "puma", "municipality_fips", "municipality", "commissioner_dist",
     "reps_dist", "senate_dist", "ward", "ssa_name", "ssa_no", "tif_agencynum", "ohare_noise",
     "floodplain", "fs_flood_factor", "fs_flood_risk_direction", "withinmr100", "withinmr101300",
     "school_elem_district", "school_hs_district", "indicator_has_address", "indicator_has_latlon"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
     SELECT *, 1 AS retention_priority
     FROM {{ source('staging', 'cook_county_parcel_locations') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
     SELECT *,
          row_number() over(partition by
               {% for sc in source_cols %}{{ sc }},{% endfor %}
               {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
          ) as rn
     FROM {{ source('staging', 'temp_cook_county_parcel_locations') }}
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
ORDER BY pin, source_data_updated
