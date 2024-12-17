{% set dataset_name = "chicago_relocated_vehicles" %}
{% set source_cols = [
    "relocated_from_street_direction", "plate", "relocated_to_address_number",
    "relocated_to_street_name", "relocated_from_location_zip", "service_request_number",
    "relocated_from_longitude", "state", "relocated_from_street_name", "color",
    "relocated_from_location_address", "relocated_from_y_coordinate",
    "relocated_from_location_state", "relocated_reason", "relocated_date",
    "relocated_from_x_coordinate", "relocated_to_direction", "relocated_from_suffix", "make",
    "relocated_to_suffix", "relocated_from_latitude", "relocated_from_address_number",
    "relocated_from_location_city", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
