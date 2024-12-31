{% set dataset_name = "chicago_speed_camera_violations" %}
{% set ck_cols = ["camera_id", "violation_date"] %}
{% set record_id = "camera_violations_id" %}
{% set base_cols = [
    "camera_violations_id", "camera_id", "violation_date", "violations", "address", "x_coordinate",
    "y_coordinate", "longitude", "latitude", "geometry", "source_data_updated",
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
