{% set dataset_name = "cook_county_linear_water_2024" %}
{% set ck_cols = ["linearid"] %}
{% set record_id = "linearid" %}
{% set base_cols = [
    "linearid", "fullname", "mtfcc", "ansicode", "artpath", "vintage_year", "geometry",
    "source_data_updated", "ingestion_check_time"
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
