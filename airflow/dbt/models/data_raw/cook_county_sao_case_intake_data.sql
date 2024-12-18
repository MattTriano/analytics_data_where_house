{% set dataset_name = "cook_county_sao_case_intake_data" %}
{% set source_cols = [
    "case_id", "case_participant_id", "received_date", "offense_category", "participant_status",
    "age_at_incident", "race", "gender", "incident_city", "incident_begin_date",
    "incident_end_date", "law_enforcement_agency", "law_enforcement_unit", "arrest_date",
    "felony_review_date", "felony_review_result", "update_offense_category"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
