{% set dataset_name = "chicago_traffic_congestion" %}
{% set ck_cols = ["segmentid"] %}
{% set record_id = "segmentid" %}
{% set base_cols = [
    "segmentid", "street", "direction", "from_street", "to_street", "length", "street_heading",
    "comments", "start_longitude", "start_latitude", "end_longitude", "end_latitude",
    "current_speed", "last_updated", "source_data_updated", "ingestion_check_time"
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
