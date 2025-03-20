{% set dataset_name = "chicago_traffic_congestion" %}
{% set source_cols = [
    "segmentid", "street", "direction", "from_street", "to_street", "length", "_street_heading",
    "_comments", "start_longitude", "_start_latitude", "end_longitude", "_end_latitude",
    "_current_speed", "_last_updated"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

