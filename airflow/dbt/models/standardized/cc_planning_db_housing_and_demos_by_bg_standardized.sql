{{ config(materialized='view') }}
{% set ck_cols = ["gidbg"] %}
{% set record_id = "gidbg" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(gidbg::text)                              AS gidbg,
        upper(state::text)                              AS state,
        upper(county::text)                             AS county,
        upper(tract::text)                              AS tract,
        upper(block_group::text)                        AS block_group,
        renter_occp_hu_cen_2010::bigint                 AS renter_occp_hu_cen_2010,
        renter_occp_hu_acs_14_18::bigint                AS renter_occp_hu_acs_14_18,
        owner_occp_hu_acs_14_18::bigint                 AS owner_occp_hu_acs_14_18,
        single_unit_acs_14_18::bigint                   AS single_unit_acs_14_18,
        avg_tot_prns_in_hhd_cen_2010::double precision  AS avg_tot_prns_in_hhd_cen_2010,
        tot_housing_units_cen_2010::bigint              AS tot_housing_units_cen_2010,
        tot_vacant_units_cen_2010::bigint               AS tot_vacant_units_cen_2010,
        tot_occp_units_cen_2010::bigint                 AS tot_occp_units_cen_2010,
        avg_tot_prns_in_hhd_acs_14_18::double precision AS avg_tot_prns_in_hhd_acs_14_18,
        tot_housing_units_acs_14_18::bigint             AS tot_housing_units_acs_14_18,
        tot_vacant_units_acs_14_18::bigint              AS tot_vacant_units_acs_14_18,
        tot_occp_units_acs_14_18::bigint                AS tot_occp_units_acs_14_18,
        mlt_u2_9_strc_acs_14_18::bigint                 AS mlt_u2_9_strc_acs_14_18,
        mlt_u10p_acs_14_18::bigint                      AS mlt_u10p_acs_14_18,
        no_plumb_acs_14_18::bigint                      AS no_plumb_acs_14_18,
        recent_built_hu_acs_14_18::bigint               AS recent_built_hu_acs_14_18,
        tot_population_acs_14_18::bigint                AS tot_population_acs_14_18,
        median_age_acs_14_18::double precision          AS median_age_acs_14_18,
        pop_65plus_acs_14_18::bigint                    AS pop_65plus_acs_14_18,
        pop_5_17_acs_14_18::bigint                      AS pop_5_17_acs_14_18,
        pop_18_24_acs_14_18::bigint                     AS pop_18_24_acs_14_18,
        pop_25_44_acs_14_18::bigint                     AS pop_25_44_acs_14_18,
        pop_45_64_acs_14_18::bigint                     AS pop_45_64_acs_14_18,
        females_acs_14_18::bigint                       AS females_acs_14_18,
        males_acs_14_18::bigint                         AS males_acs_14_18,
        avg_agg_hh_inc_acs_14_18::money                 AS avg_agg_hh_inc_acs_14_18,
        aggregate_hh_inc_acs_14_18::money               AS aggregate_hh_inc_acs_14_18,
        med_hhd_inc_bg_acs_14_18::money                 AS med_hhd_inc_bg_acs_14_18,
        pub_asst_inc_acs_14_18::bigint                  AS pub_asst_inc_acs_14_18,
        diff_hu_1yr_ago_acs_14_18::bigint               AS diff_hu_1yr_ago_acs_14_18,
        hhd_moved_in_acs_14_18::bigint                  AS hhd_moved_in_acs_14_18,
        hhd_ppl_und_18_acs_14_18::bigint                AS hhd_ppl_und_18_acs_14_18,
        female_no_hb_cen_2010::bigint                   AS female_no_hb_cen_2010,
        rel_family_hhd_cen_2010::bigint                 AS rel_family_hhd_cen_2010,
        nonfamily_hhd_acs_14_18::bigint                 AS nonfamily_hhd_acs_14_18,
        rel_family_hhd_acs_14_18::bigint                AS rel_family_hhd_acs_14_18,
        mrdcple_fmly_hhd_acs_14_18::bigint              AS mrdcple_fmly_hhd_acs_14_18,
        not_mrdcple_hhd_acs_14_18::bigint               AS not_mrdcple_hhd_acs_14_18,
        female_no_hb_acs_14_18::bigint                  AS female_no_hb_acs_14_18,
        sngl_prns_hhd_acs_14_18::bigint                 AS sngl_prns_hhd_acs_14_18,
        dataset_base_url::text                          AS dataset_base_url,
        dataset_id::bigint                              AS dataset_id,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cc_planning_db_housing_and_demos_by_bg') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
