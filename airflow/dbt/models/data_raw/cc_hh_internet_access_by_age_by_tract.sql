{% set dataset_name = "cc_hh_internet_access_by_age_by_tract" %}
{% set source_cols = [
    "b28005_001e", "b28005_001ea", "b28005_001m", "b28005_001ma", "b28005_002e", "b28005_002ea",
    "b28005_002m", "b28005_002ma", "b28005_003e", "b28005_003ea", "b28005_003m", "b28005_003ma",
    "b28005_004e", "b28005_004ea", "b28005_004m", "b28005_004ma", "b28005_005e", "b28005_005ea",
    "b28005_005m", "b28005_005ma", "b28005_006e", "b28005_006ea", "b28005_006m", "b28005_006ma",
    "b28005_007e", "b28005_007ea", "b28005_007m", "b28005_007ma", "b28005_008e", "b28005_008ea",
    "b28005_008m", "b28005_008ma", "b28005_009e", "b28005_009ea", "b28005_009m", "b28005_009ma",
    "b28005_010e", "b28005_010ea", "b28005_010m", "b28005_010ma", "b28005_011e", "b28005_011ea",
    "b28005_011m", "b28005_011ma", "b28005_012e", "b28005_012ea", "b28005_012m", "b28005_012ma",
    "b28005_013e", "b28005_013ea", "b28005_013m", "b28005_013ma", "b28005_014e", "b28005_014ea",
    "b28005_014m", "b28005_014ma", "b28005_015e", "b28005_015ea", "b28005_015m", "b28005_015ma",
    "b28005_016e", "b28005_016ea", "b28005_016m", "b28005_016ma", "b28005_017e", "b28005_017ea",
    "b28005_017m", "b28005_017ma", "b28005_018e", "b28005_018ea", "b28005_018m", "b28005_018ma",
    "b28005_019e", "b28005_019ea", "b28005_019m", "b28005_019ma", "geo_id", "name", "state",
    "county", "tract", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}
