{% set dataset_name = "chicago_food_inspections" %}
{% set source_cols = [
    "location_state", "facility_type", "city", "location_zip", "inspection_id", "license_",
    "latitude", "zip", "state", "location_address", "location_city", "aka_name", "risk",
    "longitude", "dba_name", "inspection_date", "results", "inspection_type", "address",
    "violations", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
