{% set dataset_name = "cook_county_sao_case_initiation_data" %}
{% set ck_cols = ["case_participant_id", "charge_id"] %}
{% set record_id = "case_initiation_id" %}
{% set base_cols = [
    "case_initiation_id", "case_participant_id", "case_id", "charge_id", "charge_version_id",
    "received_date", "offense_category", "charge_offense_title", "primary_charge_flag",
    "charge_count", "chapter", "act", "section", "class", "aoic", "event", "event_date",
    "arraignment_date", "finding_no_probable_cause", "bond_date_initial", "bond_date_current",
    "bond_type_initial", "bond_type_current", "bond_amount_initial", "bond_amount_current",
    "bond_electronic_monitor_flag_initial", "bond_electroinic_monitor_flag_current",
    "age_at_incident", "race", "gender", "incident_city", "incident_begin_date",
    "incident_end_date", "law_enforcement_agency", "law_enforcement_unit", "arrest_date",
    "felony_review_date", "felony_review_result", "updated_offense_category", "source_data_updated",
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
