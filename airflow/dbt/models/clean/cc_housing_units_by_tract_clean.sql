{% set dataset_name = "cc_housing_units_by_tract" %}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}
{% set base_cols = [
    "geo_id", "name", "b25001_001e", "b25001_001ea", "b25001_001m", "b25001_001ma", "state",
    "county", "tract", "dataset_base_url", "dataset_id", "source_data_updated",
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
