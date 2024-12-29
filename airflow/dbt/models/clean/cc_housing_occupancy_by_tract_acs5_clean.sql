{% set dataset_name = "cc_housing_occupancy_by_tract_acs5" %}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}
{% set base_cols = [
    "geo_id", "name", "b25001_001e", "b25001_001ea", "b25001_001m", "b25001_001ma",
    "b25002_001e", "b25002_001ea", "b25002_001m", "b25002_001ma", "b25002_002e", "b25002_002ea",
    "b25002_002m", "b25002_002ma", "b25002_003e", "b25002_003ea", "b25002_003m", "b25002_003ma",
    "b25004_001e", "b25004_001ea", "b25004_001m", "b25004_001ma", "b25004_002e", "b25004_002ea",
    "b25004_002m", "b25004_002ma", "b25004_003e", "b25004_003ea", "b25004_003m", "b25004_003ma",
    "b25004_004e", "b25004_004ea", "b25004_004m", "b25004_004ma", "b25004_005e", "b25004_005ea",
    "b25004_005m", "b25004_005ma", "b25004_006e", "b25004_006ea", "b25004_006m", "b25004_006ma",
    "b25004_007e", "b25004_007ea", "b25004_007m", "b25004_007ma", "b25004_008e", "b25004_008ea",
    "b25004_008m", "b25004_008ma", "state", "county", "tract", "dataset_base_url", "dataset_id",
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
