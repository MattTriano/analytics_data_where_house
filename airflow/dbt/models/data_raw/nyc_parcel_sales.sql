{% set dataset_name = "nyc_parcel_sales" %}
{% set source_cols = [
    "borough", "neighborhood", "building_class_category", "tax_class_at_present", "block", "lot",
    "ease_ment", "building_class_at_present", "address", "apartment_number", "zip_code",
    "residential_units", "commercial_units", "total_units", "land_square_feet", "gross_square_feet",
    "year_built", "tax_class_at_time_of_sale", "building_class_at_time_of_sale", "sale_price",
    "sale_date"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
