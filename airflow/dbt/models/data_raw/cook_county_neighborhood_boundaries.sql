{% set dataset_name = "cook_county_neighborhood_boundaries" %}
{% set source_cols = [
    "triad_name", "town_nbhd", "township_code", "triad_code", "township_name", "nbhd", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
