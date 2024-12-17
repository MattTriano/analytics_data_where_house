{% set dataset_name = "cook_county_parcel_assessment_appeals" %}
{% set source_cols = [
    "nochangereasondescription", "changereasondescription", "bor_improvementvalue", "majorclass",
    "assessor_totalvalue", "appealseq", "appellant_zip", "pin_dash", "changereason",
    "attorney_firmname", "tax_year", "appellant_state", "taxcode", "appellant", "bor_totalvalue",
    "attorneycode", "rowid", "result", "attny", "ycoord_crs_3435", "bor_landvalue", "township_code",
    "appealtrk", "pin10", "long", "vol", "assessor_improvementvalue", "appealtype",
    "appellant_address", "class", "appealtypedescription", "appellant_city", "attorney_lastname",
    "xcoord_crs_3435", "appealid", "pin", "nochangereason", "assessor_landvalue", "lat",
    "attorney_firstname", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
