{% set dataset_name = "chicago_towed_vehicles" %}
{% set source_cols = [
    "Tow_Date", "Make", "Style", "Model", "Color", "Plate", "State", "Towed_to_Address",
    "Tow_Facility_Phone", "Inventory_Number"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
