{{ config(materialized='view') }}
{% set ck_cols = ["unique_id"] %}
{% set record_id = "unique_id" %}
{% set base_cols = [
    "unique_id", "case_number", "date", "hour", "day_of_week", "month", "updated", "block",
    "zip_code", "community_area", "beat", "district", "area", "ward", "state_house_district",
    "state_senate_district", "gunshot_injury_i", "incident_primary", "incident_iucr_secondary",
    "incident_iucr_cd", "incident_fbi_cd", "incident_fbi_descr", "victimization_primary",
    "victimization_iucr_secondary", "victimization_iucr_cd", "victimization_fbi_cd",
    "victimization_fbi_descr", "location_description", "age", "sex", "race",
    "homicide_victim_first_name", "homicide_victim_mi", "homicide_victim_last_name",
    "street_outreach_organization", "latitude", "longitude", "geometry", "source_data_updated",
    "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('chicago_homicide_and_shooting_victimizations_standardized') }}
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

-- selects the source_data_updated (ie the date of publication) value from each record's
--   first ingestion into the local data warehouse
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
