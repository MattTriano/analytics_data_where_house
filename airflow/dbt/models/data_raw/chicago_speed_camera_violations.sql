{% set dataset_name = "chicago_speed_camera_violations" %}
{% set source_cols = [
    "location_state", "location_zip", "x_coordinate", "latitude", "location_address",
    "location_city", "longitude", "y_coordinate", "address", "violation_date", "violations",
    "camera_id", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

