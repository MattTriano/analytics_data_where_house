{% set dataset_name = "cook_county_sao_case_intake_data" %}
{% set ck_cols = ["case_participant_id"] %}
{% set record_id = "case_participant_id" %}
{% set base_cols = [
    "case_participant_id", "case_id", "received_date", "incident_begin_date", "incident_end_date",
    "arrest_date", "offense_category", "participant_status", "age_at_incident", "race", "gender",
    "incident_city", "law_enforcement_agency", "law_enforcement_unit", "felony_review_date",
    "felony_review_result", "update_offense_category", "source_data_updated", "ingestion_check_time"
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
