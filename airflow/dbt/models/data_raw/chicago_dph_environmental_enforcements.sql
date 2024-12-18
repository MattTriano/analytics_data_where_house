{% set dataset_name = "chicago_dph_environmental_enforcements" %}
{% set source_cols = [
    "code_violation", "latitude", "disposition", "respondent", "ticket_no", "direction",
    "longitude", "street_name", "fine_amount", "street_number_to", "comment", "street_type",
    "address", "violation_date", "case_id", "street_number", "data_source", "case_status",
    "case_type", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
