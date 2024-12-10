{% set dataset_name = "chicago_police_beat_boundaries" %}
{% set ck_cols = ["beat_num"] %}
{% set record_id = "beat_num" %}
{% set base_cols = [
    "beat_num", "beat", "district", "sector", "geometry", "source_data_updated",
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
