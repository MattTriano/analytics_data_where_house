{% set dataset_name = "chicago_dph_environmental_enforcements" %}
{% set ck_cols = [ "ticket_no", "case_id", "respondent", "comment", "address", "code_violation"] %}
{% set record_id = "violation_notice_id" %}
{% set base_cols = [
    "violation_notice_id", "ticket_no", "case_id", "respondent", "comment", "address",
    "code_violation", "violation_date", "case_type", "disposition", "fine_amount", "case_status",
    "street_number", "street_number_to", "direction", "street_name", "street_type", "data_source",
    "latitude", "longitude", "geometry", "source_data_updated", "ingestion_check_time"
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
