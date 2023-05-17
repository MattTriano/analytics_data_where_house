{{ config(materialized='view') }}
{% set ck_cols = ["case_participant_id", "charge_id"] %}
{% set record_id = "charge_disposition_id" %}
{% set base_cols = [
    "charge_disposition_id", "case_participant_id", "charge_id", "charge_version_id",
    "charge_count", "case_id", "received_date", "offense_category", "primary_charge_flag",
    "disposition_charged_offense_title", "disposition_date", "disposition_charged_chapter",
    "disposition_charged_act", "disposition_charged_section", "disposition_charged_class",
    "disposition_charged_aoic", "charge_disposition", "charge_disposition_reason", "judge",
    "disposition_court_name", "disposition_court_facility", "age_at_incident", "race", "gender",
    "incident_city", "incident_begin_date", "incident_end_date", "law_enforcement_agency",
    "law_enforcement_unit", "arrest_date", "felony_review_date", "felony_review_result",
    "arraignment_date", "updated_offense_category", "source_data_updated", "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('cook_county_sao_case_disposition_data_standardized') }}
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
