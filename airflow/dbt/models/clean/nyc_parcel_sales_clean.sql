{% set dataset_name = "nyc_parcel_sales" %}
{% set ck_cols = ["address", "lot", "sale_price", "block", "sale_date"] %}
{% set record_id = "nyc_parcel_sale_id" %}
{% set base_cols = [
    "nyc_parcel_sale_id", "sale_price", "sale_date", "block", "lot", "borough", "address",
    "apartment_number", "zip_code", "neighborhood", "building_class_category",
    "tax_class_at_time_of_sale", "building_class_at_time_of_sale", "tax_class_at_present",
    "building_class_at_present", "residential_units", "commercial_units", "total_units",
    "land_square_feet", "gross_square_feet", "year_built", "source_data_updated",
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
