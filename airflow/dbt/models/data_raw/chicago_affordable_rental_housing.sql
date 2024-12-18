{% set dataset_name = "chicago_affordable_rental_housing" %}
{% set source_cols = [
    "location_state", "zip_code", "location_zip", "phone_number", "x_coordinate", "latitude",
    "property_type", "location_address", "location_city", "longitude", "property_name",
    "y_coordinate", "units", "community_area", "address", "community_area_number",
    "management_company", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
