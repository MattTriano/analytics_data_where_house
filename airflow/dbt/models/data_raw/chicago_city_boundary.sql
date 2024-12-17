{% set dataset_name = "chicago_city_boundary" %}
{% set source_cols = [
    "name", "objectid", "shape_area", "shape_len", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
