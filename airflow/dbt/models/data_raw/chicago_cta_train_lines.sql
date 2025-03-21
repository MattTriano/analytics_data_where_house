{% set dataset_name = "chicago_cta_train_lines" %}
{% set source_cols = [
    "description", "legend", "type", "lines", "shape_len", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
