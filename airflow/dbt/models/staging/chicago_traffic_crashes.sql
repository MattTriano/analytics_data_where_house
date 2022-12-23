{{ config(materialized='table') }}
{% set source_cols = [
    "work_zone_type", "injuries_fatal", "workers_present_i", "injuries_non_incapacitating",
    "crash_record_id", "injuries_incapacitating", "injuries_no_indication", "latitude",
    "lighting_condition", "street_no", "injuries_unknown", "device_condition", "rd_no",
    "crash_date", "private_property_i", "trafficway_type", "traffic_control_device", "road_defect",
    "damage", "crash_date_est_i", "longitude", "crash_month", "street_name", "crash_day_of_week",
    "crash_hour", "first_crash_type", "injuries_reported_not_evident", "statements_taken_i",
    "num_units", "most_severe_injury", "date_police_notified", "photos_taken_i",
    "weather_condition", "prim_contributory_cause", "hit_and_run_i", "report_type", "alignment",
    "intersection_related_i", "sec_contributory_cause", "crash_type", "beat_of_occurrence",
    "street_direction", "work_zone_i", "roadway_surface_cond", "posted_speed_limit",
    "injuries_total", "lane_cnt", "dooring_i", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('staging', 'chicago_traffic_crashes') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
    SELECT *,
        row_number() over(partition by
            {% for sc in source_cols %}{{ sc }},{% endfor %}
            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
        ) as rn
    FROM {{ source('staging', 'temp_chicago_traffic_crashes') }}
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
