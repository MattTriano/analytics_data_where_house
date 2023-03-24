{{ config(materialized='view') }}
{% set ck_cols = ["crash_record_id"] %}
{% set record_id = "crash_record_id" %}
{% set base_cols = [
    "crash_record_id", "crash_date", "street_no", "street_direction", "street_name", "crash_month",
    "crash_day_of_week", "crash_hour", "date_police_notified", "injuries_fatal",
    "injuries_non_incapacitating", "injuries_incapacitating", "injuries_no_indication",
    "injuries_unknown", "injuries_reported_not_evident", "injuries_total", "most_severe_injury",
    "damage", "num_units", "work_zone_type", "workers_present_i", "crash_date_est_i",
    "photos_taken_i", "private_property_i", "intersection_related_i", "statements_taken_i",
    "hit_and_run_i", "work_zone_i", "dooring_i", "lane_cnt", "trafficway_type",
    "traffic_control_device", "road_defect", "lighting_condition", "device_condition",
    "weather_condition", "alignment", "roadway_surface_cond", "posted_speed_limit",
    "first_crash_type", "prim_contributory_cause", "sec_contributory_cause", "rd_no",
    "beat_of_occurrence", "report_type", "crash_type", "latitude", "longitude", "geometry",
    "source_data_updated", "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('chicago_traffic_crashes_standardized') }}
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
