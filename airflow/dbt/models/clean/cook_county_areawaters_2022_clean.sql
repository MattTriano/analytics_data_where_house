{% set dataset_name = "cook_county_areawaters_2022" %}
{% set ck_cols = ["hydroid"] %}
{% set record_id = "hydroid" %}
{% set base_cols = [
    "hydroid", "vintage_year", "ansicode", "fullname", "mtfcc", "area_land", "area_water",
    "longitude", "latitude", "geometry", "source_data_updated", "ingestion_check_time"
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
