{% set dataset_name = "chicago_affordable_rental_housing" %}
{% set ck_cols = ["address", "management_company", "units", "property_type"] %}
{% set record_id = "housing_development_id" %}
{% set base_cols = [
    "housing_development_id", "address", "zip_code", "community_area", "community_area_number",
    "phone_number", "property_type", "property_name", "units", "management_company", "x_coordinate",
    "y_coordinate", "latitude", "longitude", "geometry", "source_data_updated",
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
