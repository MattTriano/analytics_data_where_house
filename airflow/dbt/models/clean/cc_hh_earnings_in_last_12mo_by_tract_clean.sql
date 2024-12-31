{% set dataset_name = "cc_hh_earnings_in_last_12mo_by_tract" %}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}
{% set base_cols = [
    "geo_id", "name", "b19051_001e", "b19051_001ea", "b19051_001m", "b19051_001ma", "b19051_002e",
    "b19051_002ea", "b19051_002m", "b19051_002ma", "b19051_003e", "b19051_003ea", "b19051_003m",
    "b19051_003ma", "state", "county", "tract", "dataset_base_url", "dataset_id",
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
