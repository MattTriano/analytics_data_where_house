{% set dataset_name = "chicago_police_stations" %}
{% set source_cols = [
    "location_state", "website", "city", "location_zip", "x_coordinate", "latitude", "zip", "state",
    "location_address", "location_city", "longitude", "fax", "y_coordinate", "address", "tty",
    "district_name", "district", "phone", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
