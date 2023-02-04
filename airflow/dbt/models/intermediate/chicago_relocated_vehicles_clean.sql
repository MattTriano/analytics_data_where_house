{{ config(materialized='view') }}
{% set ck_cols = ["service_request_number"] %}
{% set record_id = "service_request_number" %}
{% set base_cols = [
    "service_request_number", "relocated_date", "relocated_reason", "relocated_from_address_number",
    "relocated_from_street_direction", "relocated_from_street_name", "relocated_from_suffix",
    "relocated_from_longitude", "relocated_from_latitude", "relocated_from_x_coordinate",
    "relocated_from_y_coordinate", "relocated_to_address_number", "relocated_to_direction",
    "relocated_to_street_name", "relocated_to_suffix", "plate", "make", "state", "color",
    "geometry", "source_data_updated", "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('chicago_relocated_vehicles_standardized') }}
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
