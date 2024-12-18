{% set dataset_name = "chicago_bike_paths" %}
{% set source_cols = [
    "mi_ctrline", "oneway_dir", "displayrou", "contraflow", "br_ow_dir", "f_street", "br_oneway",
    "t_street", "street", "st_name", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
