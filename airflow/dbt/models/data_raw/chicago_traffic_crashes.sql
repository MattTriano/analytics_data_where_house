{% set dataset_name = "chicago_traffic_crashes" %}
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

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
