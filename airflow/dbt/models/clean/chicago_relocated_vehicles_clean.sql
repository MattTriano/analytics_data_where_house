{% set dataset_name = "chicago_relocated_vehicles" %}
{% set ck_cols = ["service_request_number"] %}
{% set record_id = "service_request_number" %}
{% set base_cols = [
    "service_request_number", "relocated_date", "relocated_reason", "relocated_from_address_number",
    "relocated_from_street_direction", "relocated_from_street_name", "relocated_from_suffix",
    "relocated_from_longitude", "relocated_from_latitude", "relocated_from_x_coordinate",
    "relocated_from_y_coordinate", "relocated_to_address_number", "relocated_to_direction",
    "relocated_to_street_name", "relocated_to_suffix", "plate", "make", "state", "color",
    "geometry", "source_data_updated", "ingestion_check_time"
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
