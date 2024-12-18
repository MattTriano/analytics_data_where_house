{% set dataset_name = "cook_county_multifam_parcel_improvements" %}
{% set source_cols = [
    "pin", "tax_year", "card_num", "class", "township_code", "cdu", "pin_is_multicard",
    "pin_num_cards", "pin_is_multiland", "pin_num_landlines", "year_built", "building_sqft",
    "land_sqft", "num_bedrooms", "num_rooms", "num_full_baths", "num_half_baths", "num_fireplaces",
    "type_of_residence", "construction_quality", "num_apartments", "attic_finish",
    "garage_attached", "garage_area_included", "garage_size", "garage_ext_wall_material",
    "attic_type", "basement_type", "ext_wall_material", "central_heating", "repair_condition",
    "basement_finish", "roof_material", "single_v_multi_family", "site_desirability",
    "num_commercial_units", "renovation", "recent_renovation", "porch", "central_air", "design_plan"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
