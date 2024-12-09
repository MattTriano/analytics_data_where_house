{% set dataset_name = "chicago_divvy_stations" %}
{% set ck_cols = ["id"] %}
{% set record_id = "id" %}
{% set base_cols = [
    "id", "station_name", "short_name", "status", "latitude", "longitude", "docks_in_service",
    "total_docks", "geometry", "source_data_updated", "ingestion_check_time"
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
