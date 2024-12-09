{% set dataset_name = "chicago_towed_vehicles" %}
{% set ck_cols = ["inventory_number", "tow_date", "plate"] %}
{% set record_id = "vehicle_tow_id" %}
{% set base_cols = [
    "vehicle_tow_id", "inventory_number", "tow_date", "make", "model", "style", "color", "plate",
    "state", "towed_to_address", "tow_facility_phone", "source_data_updated", "ingestion_check_time"
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