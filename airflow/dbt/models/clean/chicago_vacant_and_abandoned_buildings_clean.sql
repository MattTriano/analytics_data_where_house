{% set dataset_name = "chicago_vacant_and_abandoned_buildings" %}
{% set ck_cols = ["violation_number", "property_address", "entity_or_person_s_"] %}
{% set record_id = "vacant_bldg_violation_id" %}
{% set base_cols = [
    "vacant_bldg_violation_id", "violation_number", "property_address", "entity_or_persons",
    "issued_date", "violation_type", "docket_number", "last_hearing_date", "issuing_department",
    "disposition_description", "total_fines", "interest_amount", "total_administrative_costs",
    "original_total_amount_due", "collection_costs_or_attorney_fees", "court_cost", "total_paid",
    "current_amount_due", "latitude", "longitude", "geometry", "source_data_updated",
    "ingestion_check_time"
] %}
{% set updated_at_col = "source_data_updated" %}

{% set query = generate_clean_stage_incremental_dedupe_query(
    dataset_name=dataset_name,
    record_id=record_id,
    ck_cols=ck_cols,
    base_cols=base_cols,
    updated_at_col=updated_at_col
) %}

{{ query }}
