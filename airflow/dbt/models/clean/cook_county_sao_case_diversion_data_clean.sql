{% set dataset_name = "cook_county_sao_case_diversion_data" %}
{% set ck_cols = ["case_participant_id", "diversion_program"] %}
{% set record_id = "participant_diversion_id" %}
{% set base_cols = [
    "participant_diversion_id", "case_participant_id", "diversion_program", "case_id",
    "received_date", "referral_date", "offense_category", "primary_charge_offense_title", "statute",
    "race", "gender", "diversion_count", "diversion_result", "diversion_closed_date",
    "source_data_updated", "ingestion_check_time"
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
