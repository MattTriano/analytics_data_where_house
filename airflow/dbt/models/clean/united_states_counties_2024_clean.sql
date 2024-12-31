{% set dataset_name = "united_states_counties_2024" %}
{% set ck_cols = ["geoid"] %}
{% set record_id = "geoid" %}
{% set base_cols = [
    "geoid", "geoidfq", "countyfp", "countyns", "statefp", "name", "namelsad", "lsad", "classfp",
    "mtfcc", "csafp", "cbsafp", "metdivfp", "funcstat", "area_land", "area_water", "longitude",
    "latitude", "geometry", "vintage_year", "source_data_updated", "ingestion_check_time"
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
