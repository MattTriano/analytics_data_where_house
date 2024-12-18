{% set dataset_name = "chicago_community_area_boundaries" %}
{% set source_cols = [
    "community", "area", "shape_area", "perimeter", "area_num_1", "area_numbe", "comarea_id",
    "comarea", "shape_len", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
