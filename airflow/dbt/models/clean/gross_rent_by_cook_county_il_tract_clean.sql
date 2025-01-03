{% set dataset_name = "gross_rent_by_cook_county_il_tract" %}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}
{% set base_cols = [
    "geo_id", "name", "b25063_001e", "b25063_001ea", "b25063_001m", "b25063_001ma", "b25063_002e",
    "b25063_002ea", "b25063_002m", "b25063_002ma", "b25063_003e", "b25063_003ea", "b25063_003m",
    "b25063_003ma", "b25063_004e", "b25063_004ea", "b25063_004m", "b25063_004ma", "b25063_005e",
    "b25063_005ea", "b25063_005m", "b25063_005ma", "b25063_006e", "b25063_006ea", "b25063_006m",
    "b25063_006ma", "b25063_007e", "b25063_007ea", "b25063_007m", "b25063_007ma", "b25063_008e",
    "b25063_008ea", "b25063_008m", "b25063_008ma", "b25063_009e", "b25063_009ea", "b25063_009m",
    "b25063_009ma", "b25063_010e", "b25063_010ea", "b25063_010m", "b25063_010ma", "b25063_011e",
    "b25063_011ea", "b25063_011m", "b25063_011ma", "b25063_012e", "b25063_012ea", "b25063_012m",
    "b25063_012ma", "b25063_013e", "b25063_013ea", "b25063_013m", "b25063_013ma", "b25063_014e",
    "b25063_014ea", "b25063_014m", "b25063_014ma", "b25063_015e", "b25063_015ea", "b25063_015m",
    "b25063_015ma", "b25063_016e", "b25063_016ea", "b25063_016m", "b25063_016ma", "b25063_017e",
    "b25063_017ea", "b25063_017m", "b25063_017ma", "b25063_018e", "b25063_018ea", "b25063_018m",
    "b25063_018ma", "b25063_019e", "b25063_019ea", "b25063_019m", "b25063_019ma", "b25063_020e",
    "b25063_020ea", "b25063_020m", "b25063_020ma", "b25063_021e", "b25063_021ea", "b25063_021m",
    "b25063_021ma", "b25063_022e", "b25063_022ea", "b25063_022m", "b25063_022ma", "b25063_023e",
    "b25063_023ea", "b25063_023m", "b25063_023ma", "b25063_024e", "b25063_024ea", "b25063_024m",
    "b25063_024ma", "b25063_025e", "b25063_025ea", "b25063_025m", "b25063_025ma", "b25063_026e",
    "b25063_026ea", "b25063_026m", "b25063_026ma", "b25063_027e", "b25063_027ea", "b25063_027m",
    "b25063_027ma", "state", "county", "tract", "dataset_base_url", "dataset_id",
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
