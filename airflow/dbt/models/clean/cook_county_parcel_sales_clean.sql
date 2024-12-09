{% set dataset_name = "cook_county_parcel_sales" %}
{% set ck_cols = ["pin", "sale_document_num", "sale_date", "sale_price"] %}
{% set record_id = "parcel_sale_id" %}
{% set base_cols = [
    "parcel_sale_id", "pin", "sale_document_num", "sale_date", "is_mydec_date", "year",
    "sale_price", "sale_deed_type", "sale_type", "class", "township_code", "is_multisale",
    "num_parcels_sale", "sale_seller_name", "sale_buyer_name", "source_data_updated",
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
