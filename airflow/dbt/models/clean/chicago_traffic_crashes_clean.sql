{% set dataset_name = "chicago_traffic_crashes" %}
{% set ck_cols = ["crash_record_id"] %}
{% set record_id = "crash_record_id" %}
{% set base_cols = [
    "crash_record_id", "crash_date", "rd_no", "street_no", "street_direction", "street_name",
    "crash_month", "crash_day_of_week", "crash_hour", "date_police_notified", "injuries_fatal",
    "injuries_non_incapacitating", "injuries_incapacitating", "injuries_no_indication",
    "injuries_unknown", "injuries_reported_not_evident", "injuries_total", "most_severe_injury",
    "damage", "num_units", "work_zone_type", "workers_present_i", "crash_date_est_i",
    "photos_taken_i", "private_property_i", "intersection_related_i", "statements_taken_i",
    "hit_and_run_i", "work_zone_i", "dooring_i", "lane_cnt", "trafficway_type",
    "traffic_control_device", "road_defect", "lighting_condition", "device_condition",
    "weather_condition", "alignment", "roadway_surface_cond", "posted_speed_limit",
    "first_crash_type", "prim_contributory_cause", "sec_contributory_cause", "beat_of_occurrence",
    "report_type", "crash_type", "latitude", "longitude", "geometry", "source_data_updated",
    "ingestion_check_time"
] %}
{% set updated_at_col = "source_data_updated" %}

{% set query = generate_clean_stage_incremental_dedupe_query(
    dataset_name=dataset_name,
    record_id=record_id,
    ck_cols=ck_cols,
    base_cols=base_cols,
    updated_at_col=updated_at_col
) %}

{{ query }}
