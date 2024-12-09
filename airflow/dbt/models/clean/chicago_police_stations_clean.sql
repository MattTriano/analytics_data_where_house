{% set dataset_name = "chicago_police_stations" %}
{% set ck_cols = ["district_name"] %}
{% set record_id = "district_name" %}
{% set base_cols = [
    "district_name", "district", "address", "city", "zip", "state", "phone", "tty", "fax",
    "website", "x_coordinate", "y_coordinate", "latitude", "longitude", "geometry",
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
