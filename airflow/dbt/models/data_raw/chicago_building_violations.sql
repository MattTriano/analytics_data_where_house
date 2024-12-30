{% set dataset_name = "chicago_building_violations" %}
{% set source_cols = [
    "location_state", "location_zip", "violation_status_date", "violation_last_modified_date",
    "latitude", "inspection_waived", "violation_code", "location_address", "inspection_number",
    "location_city", "violation_location", "violation_inspector_comments", "inspection_category",
    "longitude", "street_name", "inspector_id", "violation_ordinance", "id",
    "violation_description", "department_bureau", "street_type", "address", "violation_date",
    "property_group", "violation_status", "street_direction", "ssa", "street_number",
    "inspection_status", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

