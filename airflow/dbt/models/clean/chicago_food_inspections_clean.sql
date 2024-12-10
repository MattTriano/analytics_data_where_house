{% set dataset_name = "chicago_food_inspections" %}
{% set ck_cols = ["inspection_id"] %}
{% set record_id = "inspection_id" %}
{% set base_cols = [
    "inspection_id", "license_id", "dba_name", "aka_name", "inspection_date", "inspection_type",
    "violations", "results", "facility_type", "risk", "address", "city", "zip", "state", "latitude",
    "longitude", "geometry", "source_data_updated", "ingestion_check_time"
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
