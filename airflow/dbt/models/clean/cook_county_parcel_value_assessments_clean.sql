{% set dataset_name = "cook_county_parcel_value_assessments" %}
{% set ck_cols = ["pin", "tax_year"] %}
{% set record_id = "parcel_assessment_id" %}
{% set base_cols = [
    "parcel_assessment_id", "pin", "tax_year", "class", "township_code", "township_name",
    "mailed_bldg", "mailed_land", "mailed_tot", "certified_bldg", "certified_land", "certified_tot",
    "board_bldg", "board_land", "board_tot", "source_data_updated", "ingestion_check_time"
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
