{% set dataset_name = "cook_county_parcel_value_assessments" %}
{% set source_cols = [
    "pin", "tax_year", "class", "township_code", "township_name", "mailed_bldg", "mailed_land",
    "mailed_tot", "certified_bldg", "certified_land", "certified_tot", "board_bldg", "board_land",
    "board_tot"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
