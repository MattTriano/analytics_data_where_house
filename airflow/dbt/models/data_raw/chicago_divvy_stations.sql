{% set dataset_name = "chicago_divvy_stations" %}
{% set source_cols = [
    "total_docks", "latitude", "station_name", "longitude", "id", "docks_in_service", "status",
    "short_name", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
