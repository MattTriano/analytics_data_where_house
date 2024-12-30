{% set dataset_name = "cc_housing_tenure_single_race_by_tract_acs5" %}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}
{% set base_cols = [
    "geo_id", "name", "b25003_001e", "b25003_001ea", "b25003_001m", "b25003_001ma", "b25003_002e",
    "b25003_002ea", "b25003_002m", "b25003_002ma", "b25003_003e", "b25003_003ea", "b25003_003m",
    "b25003_003ma", "b25003a_001e", "b25003a_001ea", "b25003a_001m", "b25003a_001ma",
    "b25003a_002e", "b25003a_002ea", "b25003a_002m", "b25003a_002ma", "b25003a_003e",
    "b25003a_003ea", "b25003a_003m", "b25003a_003ma", "b25003b_001e", "b25003b_001ea",
    "b25003b_001m", "b25003b_001ma", "b25003b_002e", "b25003b_002ea", "b25003b_002m",
    "b25003b_002ma", "b25003b_003e", "b25003b_003ea", "b25003b_003m", "b25003b_003ma",
    "b25003d_001e", "b25003d_001ea", "b25003d_001m", "b25003d_001ma", "b25003d_002e",
    "b25003d_002ea", "b25003d_002m", "b25003d_002ma", "b25003d_003e", "b25003d_003ea",
    "b25003d_003m", "b25003d_003ma", "state", "county", "tract", "dataset_base_url", "dataset_id",
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
