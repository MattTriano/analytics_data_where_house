{% set dataset_name = "chicago_vacant_and_abandoned_buildings" %}
{% set source_cols = [
    "issued_date", "disposition_description", "latitude", "total_administrative_costs",
    "entity_or_person_s_", "total_paid", "last_hearing_date", "total_fines", "current_amount_due",
    "longitude", "docket_number", "original_total_amount_due", "collection_costs_or_attorney_fees",
    "property_address", "court_cost", "issuing_department", "violation_type", "violation_number",
    "interest_amount", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
