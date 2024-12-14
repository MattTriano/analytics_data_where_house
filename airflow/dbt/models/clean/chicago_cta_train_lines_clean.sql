{% set dataset_name = "chicago_cta_train_lines" %}
{% set ck_cols = ["description"] %}
{% set record_id = "description" %}
{% set base_cols = [
    "description", "legend", "type", "lines", "shape_len", "geometry", "source_data_updated",
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
