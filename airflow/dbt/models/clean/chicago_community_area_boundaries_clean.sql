{% set dataset_name = "chicago_community_area_boundaries" %}
{% set ck_cols = ["area_numbe"] %}
{% set record_id = "community_area_id" %}
{% set base_cols = [
    "community_area_id", "area_numbe", "community", "shape_len", "shape_area", "geometry",
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
