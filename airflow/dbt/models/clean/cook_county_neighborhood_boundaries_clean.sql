{% set dataset_name = "cook_county_neighborhood_boundaries" %}
{% set ck_cols = ["town_nbhd", "triad_code"] %}
{% set record_id = "nbhd_boundary_id" %}
{% set base_cols = [
    "nbhd_boundary_id", "town_nbhd", "triad_code", "triad_name", "township_code", "nbhd",
    "township_name", "geometry", "source_data_updated", "ingestion_check_time"
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
