{% set dataset_name = "cook_county_parcel_assessment_appeals" %}
{% set ck_cols = ["rowid"] %}
{% set record_id = "rowid" %}
{% set base_cols = [
    "rowid", "appealid", "appealtrk", "appealseq", "pin", "pin_dash", "pin10", "township_code",
    "tax_year", "class", "majorclass", "result", "changereason", "changereasondescription",
    "nochangereason", "nochangereasondescription", "bor_landvalue", "bor_improvementvalue",
    "bor_totalvalue", "assessor_landvalue", "assessor_improvementvalue", "assessor_totalvalue",
    "taxcode", "vol", "appealtype", "appealtypedescription", "appellant", "appellant_address",
    "appellant_city", "appellant_zip", "appellant_state", "attny", "attorneycode",
    "attorney_firmname", "attorney_firstname", "attorney_lastname", "xcoord_crs_3435",
    "ycoord_crs_3435", "lat", "long", "geometry", "source_data_updated", "ingestion_check_time"
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
