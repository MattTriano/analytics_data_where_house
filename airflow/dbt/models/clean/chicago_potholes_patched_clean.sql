{% set dataset_name = "chicago_potholes_patched" %}
{% set ck_cols = ["address", "request_date", "completion_date"] %}
{% set record_id = "pothole_repair_id" %}
{% set base_cols = [
    "pothole_repair_id", "request_date", "completion_date", "address",
    "number_of_potholes_filled_on_block", "latitude", "longitude", "geometry",
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
