{% set dataset_name = "cook_county_sao_case_diversion_data" %}
{% set source_cols = [
    "case_id", "case_participant_id", "received_date", "offense_category", "diversion_program",
    "referral_date", "diversion_count", "primary_charge_offense_title", "statute", "race", "gender",
    "diversion_result", "diversion_closed_date"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
