{% set dataset_name = "chicago_cta_train_stations" %}
{% set ck_cols = ["stop_id"] %}
{% set record_id = "stop_id" %}
{% set base_cols = [
    "stop_id", "station_name", "station_descriptive_name", "direction_id", "map_id", "stop_name",
    "red_line", "orange_line", "pink_line", "yellow_line", "green_line", "blue_line", "purple_line",
    "purple_express", "brown_line", "ada", "geometry", "source_data_updated", "ingestion_check_time"
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
