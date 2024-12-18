{% set dataset_name = "chicago_potholes_patched" %}
{% set source_cols = [
    "completion_date", "latitude", "request_date", "longitude", "address",
    "number_of_potholes_filled_on_block", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
