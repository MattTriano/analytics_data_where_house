{% set dataset_name = "cook_county_multifam_parcel_improvements" %}
{% set ck_cols = ["pin", "tax_year", "card_num"] %}
{% set record_id = "res_parcel_improvement_id" %}
{% set base_cols = [
    "res_parcel_improvement_id", "pin", "tax_year", "card_num", "class", "single_v_multi_family",
    "township_code", "cdu", "renovation", "recent_renovation", "pin_is_multicard", "pin_num_cards",
    "pin_is_multiland", "pin_num_landlines", "year_built", "building_sqft", "land_sqft",
    "num_bedrooms", "num_rooms", "num_full_baths", "num_half_baths", "num_fireplaces",
    "num_commercial_units", "num_apartments", "type_of_residence", "construction_quality",
    "garage_attached", "garage_area_included", "garage_size", "garage_ext_wall_material",
    "ext_wall_material", "roof_material", "attic_type", "attic_finish", "basement_type",
    "basement_finish", "repair_condition", "central_heating", "site_desirability", "porch",
    "central_air", "design_plan", "source_data_updated", "ingestion_check_time"
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
