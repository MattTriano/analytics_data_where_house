{% set dataset_name = "cook_county_sao_case_disposition_data" %}
{% set ck_cols = ["case_participant_id", "charge_id"] %}
{% set record_id = "charge_disposition_id" %}
{% set base_cols = [
    "charge_disposition_id", "case_participant_id", "charge_id", "charge_version_id",
    "charge_count", "case_id", "received_date", "offense_category", "primary_charge_flag",
    "disposition_charged_offense_title", "disposition_date", "disposition_charged_chapter",
    "disposition_charged_act", "disposition_charged_section", "disposition_charged_class",
    "disposition_charged_aoic", "charge_disposition", "charge_disposition_reason", "judge",
    "disposition_court_name", "disposition_court_facility", "age_at_incident", "race", "gender",
    "incident_city", "incident_begin_date", "incident_end_date", "law_enforcement_agency",
    "law_enforcement_unit", "arrest_date", "felony_review_date", "felony_review_result",
    "arraignment_date", "updated_offense_category", "source_data_updated", "ingestion_check_time"
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
