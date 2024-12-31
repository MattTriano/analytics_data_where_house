{% set dataset_name = "united_states_counties_2022" %}
{% set ck_cols = ["geoid"] %}
{% set record_id = "geoid" %}
{% set base_cols = [
    "geoid", "vintage_year", "statefp", "countyfp", "countyns", "name", "namelsad", "lsad",
    "classfp", "mtfcc", "csafp", "cbsafp", "metdivfp", "funcstat", "area_land", "area_water",
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
