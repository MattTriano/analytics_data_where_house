{% set dataset_name = "chicago_bike_paths" %}
{% set ck_cols = ["st_name", "f_street", "t_street", "br_ow_dir"] %}
{% set record_id = "bike_route_segment_id" %}
{% set base_cols = [
    "bike_route_segment_id", "st_name", "f_street", "t_street", "street", "displayrou",
    "oneway_dir", "contraflow", "br_ow_dir", "br_oneway", "mi_ctrline", "geometry",
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
