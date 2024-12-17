{% set dataset_name = "cook_county_sao_case_initiation_data" %}
{% set source_cols = [
    "case_id", "case_participant_id", "received_date", "offense_category", "primary_charge_flag",
    "charge_id", "charge_version_id", "charge_offense_title", "charge_count", "chapter", "act",
    "section", "class", "aoic", "event", "event_date", "finding_no_probable_cause",
    "arraignment_date", "bond_date_initial", "bond_date_current", "bond_type_initial",
    "bond_type_current", "bond_amount_initial", "bond_amount_current",
    "bond_electronic_monitor_flag_initial", "bond_electroinic_monitor_flag_current",
    "age_at_incident", "race", "gender", "incident_city", "incident_begin_date",
    "incident_end_date", "law_enforcement_agency", "law_enforcement_unit", "arrest_date",
    "felony_review_date", "felony_review_result", "updated_offense_category"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
