{{ config(materialized='table') }}
{% set source_cols = [
    "state", "county", "tract", "block_group", "gidbg", "renter_occp_hu_cen_2010",
    "renter_occp_hu_acs_14_18", "owner_occp_hu_acs_14_18", "single_unit_acs_14_18",
    "avg_tot_prns_in_hhd_cen_2010", "tot_housing_units_cen_2010", "tot_vacant_units_cen_2010",
    "tot_occp_units_cen_2010", "avg_tot_prns_in_hhd_acs_14_18", "tot_housing_units_acs_14_18",
    "tot_vacant_units_acs_14_18", "tot_occp_units_acs_14_18", "mlt_u2_9_strc_acs_14_18",
    "mlt_u10p_acs_14_18", "no_plumb_acs_14_18", "recent_built_hu_acs_14_18",
    "tot_population_acs_14_18", "median_age_acs_14_18", "pop_65plus_acs_14_18",
    "pop_5_17_acs_14_18", "pop_18_24_acs_14_18", "pop_25_44_acs_14_18", "pop_45_64_acs_14_18",
    "females_acs_14_18", "males_acs_14_18", "avg_agg_hh_inc_acs_14_18",
    "aggregate_hh_inc_acs_14_18", "med_hhd_inc_bg_acs_14_18", "pub_asst_inc_acs_14_18",
    "diff_hu_1yr_ago_acs_14_18", "hhd_moved_in_acs_14_18", "hhd_ppl_und_18_acs_14_18",
    "female_no_hb_cen_2010", "rel_family_hhd_cen_2010", "nonfamily_hhd_acs_14_18",
    "rel_family_hhd_acs_14_18", "mrdcple_fmly_hhd_acs_14_18", "not_mrdcple_hhd_acs_14_18",
    "female_no_hb_acs_14_18", "sngl_prns_hhd_acs_14_18", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('data_raw', 'cc_planning_db_housing_and_demos_by_bg') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
    SELECT *,
        row_number() over(partition by
            {% for sc in source_cols %}{{ sc }},{% endfor %}
            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
        ) as rn
    FROM {{ source('data_raw', 'temp_cc_planning_db_housing_and_demos_by_bg') }}
),
distinct_records_in_current_pull AS (
    SELECT
        {% for sc in source_cols %}{{ sc }},{% endfor %}
        {% for mc in metadata_cols %}{{ mc }},{% endfor %}
        2 AS retention_priority
    FROM current_pull_with_distinct_combos_numbered
    WHERE rn = 1
),

-- stacking the existing data with all distinct records from the latest pull
data_raw_table_with_all_new_and_updated_records AS (
    SELECT *
    FROM records_in_data_raw_table
        UNION ALL
    SELECT *
    FROM distinct_records_in_current_pull
),

-- selecting records that where source columns are distinct (keeping the earlier recovery
--  when there are duplicates to chose from)
data_raw_table_with_new_and_updated_records AS (
    SELECT *,
    row_number() over(partition by
        {% for sc in source_cols %}{{ sc }}{{ "," if not loop.last }}{% endfor %}
        ORDER BY retention_priority
        ) as rn
    FROM data_raw_table_with_all_new_and_updated_records
),
distinct_records_for_data_raw_table AS (
    SELECT
        {% for sc in source_cols %}{{ sc }},{% endfor %}
        {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
    FROM data_raw_table_with_new_and_updated_records
    WHERE rn = 1
)

SELECT *
FROM distinct_records_for_data_raw_table
