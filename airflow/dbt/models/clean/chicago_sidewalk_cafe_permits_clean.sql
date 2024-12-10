{% set dataset_name = "chicago_sidewalk_cafe_permits" %}
{% set ck_cols = ["permit_number"] %}
{% set record_id = "permit_number" %}
{% set base_cols = [
    "permit_number", "doing_business_as_name", "legal_name", "address", "account_number",
    "site_number", "issued_date", "expiration_date", "payment_date", "address_number_start",
    "address_number", "street_direction", "street", "street_type", "city", "state", "zip_code",
    "police_district", "ward", "longitude", "latitude", "geometry", "source_data_updated",
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
