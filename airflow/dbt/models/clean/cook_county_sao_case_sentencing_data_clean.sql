{% set dataset_name = "cook_county_sao_case_sentencing_data" %}
{% set ck_cols = [ "case_participant_id", "charge_id", "sentence_phase", "sentence_date", "sentence_type", "current_sentence_flag", "commitment_type", "commitment_term"] %}
{% set record_id = "charge_sentence_id" %}
{% set base_cols = [
    "charge_sentence_id", "case_participant_id", "case_id", "charge_id", "charge_version_id",
    "received_date", "offense_category", "disposition_charged_offense_title", "primary_charge_flag",
    "charge_count", "arraignment_date", "disposition_date", "disposition_charged_chapter",
    "disposition_charged_act", "disposition_charged_section", "disposition_charged_class",
    "disposition_charged_aoic", "charge_disposition", "charge_disposition_reason", "sentence_judge",
    "sentence_court_name", "sentence_court_facility", "sentence_phase", "sentence_date",
    "sentence_type", "current_sentence_flag", "commitment_type", "commitment_term",
    "commitment_unit", "length_of_case_in_days", "age_at_incident", "race", "gender",
    "incident_city", "incident_begin_date", "incident_end_date", "law_enforcement_agency",
    "law_enforcement_unit", "arrest_date", "felony_review_date", "felony_review_result",
    "updated_offense_category", "source_data_updated", "ingestion_check_time"
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
