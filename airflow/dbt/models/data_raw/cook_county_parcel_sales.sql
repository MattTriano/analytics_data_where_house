{% set dataset_name = "cook_county_parcel_sales" %}
{% set source_cols = [
     "pin", "year", "township_code", "class", "sale_date", "is_mydec_date", "sale_price",
     "sale_document_num", "sale_deed_type", "sale_seller_name", "is_multisale", "num_parcels_sale",
     "sale_buyer_name", "sale_type"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
