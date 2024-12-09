{% set dataset_name = "chicago_police_district_boundaries" %}
{% set ck_cols = ["dist_num"] %}
{% set record_id = "dist_num" %}
{% set base_cols = [
    "dist_num", "dist_label", "geometry", "source_data_updated", "ingestion_check_time"
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
