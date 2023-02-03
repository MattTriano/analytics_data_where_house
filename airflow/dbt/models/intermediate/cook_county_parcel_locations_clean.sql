{{ config(materialized='view') }}
{% set ck_cols = ["pin"] %}
{% set record_id = "parcel_location_id" %}
{% set base_cols = [
    "parcel_location_id", "pin", "property_address", "property_apt_no", "property_city",
    "property_zip", "nbhd", "township", "township_name", "municipality", "municipality_fips",
    "ward", "puma", "tract_geoid", "tract_pop", "tract_white_perc", "tract_black_perc",
    "tract_asian_perc", "tract_his_perc", "tract_other_perc", "tract_midincome",
    "commissioner_dist", "reps_dist", "senate_dist", "ssa_name", "ssa_no", "tif_agencynum",
    "school_elem_district", "school_hs_district", "mailing_address", "mailing_city",
    "mailing_zip",  "mailing_state", "ohare_noise", "floodplain", "fs_flood_factor",
    "fs_flood_risk_direction", "withinmr100", "withinmr101300", "indicator_has_address",
    "indicator_has_latlon", "longitude", "latitude", "geometry", "source_data_updated",
    "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('cook_county_parcel_locations_standardized') }}
),

-- keeps the most recently updated version of each record 
std_records_numbered_latest_first AS (
    SELECT *,
        row_number() over(partition by {{record_id}} ORDER BY source_data_updated DESC) as rn
    FROM std_data
),
most_current_records AS (
     SELECT * 
     FROM std_records_numbered_latest_first
     WHERE rn = 1
),

-- selects the source_data_updated (ie the date of publication) value from each record's first
--   ingestion into the local data warehouse 
std_records_numbered_earliest_first AS (
    SELECT *, 
        row_number() over(partition by {{record_id}} ORDER BY source_data_updated ASC) as rn
    FROM std_data
),
records_first_ingested_pub_date AS (
     SELECT {{record_id}}, source_data_updated AS first_ingested_pub_date
     FROM std_records_numbered_earliest_first
     WHERE rn = 1
)

SELECT
    {% for bc in base_cols %}mcr.{{ bc }},{% endfor %}
    fi.first_ingested_pub_date
FROM most_current_records AS mcr
LEFT JOIN records_first_ingested_pub_date AS fi
ON mcr.{{ record_id }} = fi.{{ record_id }}
ORDER BY {% for ck in ck_cols %}mcr.{{ ck }} DESC, {% endfor %} mcr.source_data_updated DESC
