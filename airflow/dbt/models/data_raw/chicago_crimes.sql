{% set dataset_name = "chicago_crimes" %}
{% set source_cols = [
    "location_state", "location_zip", "x_coordinate", "domestic", "latitude", "updated_on",
    "description", "location_address", "arrest", "location_city", "year", "longitude", "block",
    "fbi_code", "ward", "id", "date", "beat", "y_coordinate", "community_area",
    "location_description", "district", "iucr", "case_number", "primary_type", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
