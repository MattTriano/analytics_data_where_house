{% set dataset_name = "cook_county_sao_case_disposition_data" %}
{% set source_cols = [
    "case_id", "case_participant_id", "received_date", "offense_category", "primary_charge_flag",
    "charge_id", "charge_version_id", "disposition_charged_offense_title", "charge_count",
    "disposition_date", "disposition_charged_chapter", "disposition_charged_act",
    "disposition_charged_section", "disposition_charged_class", "disposition_charged_aoic",
    "charge_disposition", "charge_disposition_reason", "judge", "disposition_court_name",
    "disposition_court_facility", "age_at_incident", "race", "gender", "incident_city",
    "incident_begin_date", "incident_end_date", "law_enforcement_agency", "law_enforcement_unit",
    "arrest_date", "felony_review_date", "felony_review_result", "arraignment_date",
    "updated_offense_category"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
