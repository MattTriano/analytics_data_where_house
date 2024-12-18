{% set dataset_name = "chicago_sidewalk_cafe_permits" %}
{% set source_cols = [
    "location_state", "expiration_date", "zip_code", "address_number_start", "issued_date", "city",
    "location_zip", "police_district", "latitude", "state", "location_address", "location_city",
    "payment_date", "longitude", "account_number", "site_number", "ward", "doing_business_as_name",
    "street_type", "permit_number", "address", "legal_name", "address_number", "street",
    "street_direction", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
