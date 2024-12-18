{% set dataset_name = "chicago_cta_train_stations" %}
{% set source_cols = [
     "location_state", "location_zip", "station_descriptive_name", "blue", "station_name", "y",
     "location_address", "location_city", "brn", "direction_id", "map_id", "g", "stop_name", "p",
     "ada", "pnk", "pexp", "stop_id", "red", "o", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
