{{ config(materialized='view') }}
{% set ck_cols = ["objectid"] %}
{% set record_id = "objectid" %}

WITH records_with_basic_cleaning AS (
    SELECT
        objectid::bigint                                      AS objectid,
        upper(pre_dir::text)                                  AS pre_dir,
        upper(street_nam::text)                               AS street_name,
        upper(street_typ::text)                               AS street_type,
        upper(suf_dir::text)                                  AS suffix_dir,
        upper(oneway_dir::text)                               AS oneway_dir,
        upper(dir_travel::text)                               AS dir_travel,
        upper(tiered::text)                                   AS tiered,
        l_t_add::int                                          AS left_to_addr,
        l_f_add::int                                          AS left_from_add,
        upper(l_parity::text)                                 AS left_parity,
        upper(l_zip::text)                                    AS left_zip,
        upper(l_fips::text)                                   AS left_fips,
        upper(l_censusbl::text)                               AS left_censusbl,
        r_f_add::int                                          AS right_from_add,
        r_t_add::int                                          AS right_to_addr,
        upper(r_parity::text)                                 AS right_parity,
        upper(r_zip::text)                                    AS right_zip,
        upper(r_fips::text)                                   AS right_fips,
        upper(r_censusbl::text)                               AS right_censusbl,
        upper(t_zlev::text)                                   AS to_zlev,
        upper(f_cross::text)                                  AS from_cross,
        upper(f_cross_st::text)                               AS from_cross_st,
        upper(t_cross::text)                                  AS to_cross,
        upper(t_cross_st::text)                               AS to_cross_st,
        upper(logiclf::text)                                  AS logic_left_from,
        upper(logiclt::text)                                  AS logic_left_to,
        upper(logicrf::text)                                  AS logic_right_from,
        upper(logicrt::text)                                  AS logic_right_to,
        ewns::int                                             AS ewns,
        upper(ewns_dir::text)                                 AS ewns_dir,
        ewns_coord::int                                       AS ewns_coord,
        upper(class::text)                                    AS class,
        upper(streetname::text)                               AS street_id,
        upper(fnode_id::text)                                 AS from_node_id,
        upper(tnode_id::text)                                 AS to_node_id,
        trans_id::int                                         AS trans_id,
        upper(flag_strin::text)                               AS flag_string,
        upper(status::text)                                   AS status,
        status_dat::date                                      AS status_date,
        upper(create_use::text)                               AS create_use,
        create_tim::date                                      AS create_time,
        upper(edit_type::text)                                AS edit_type,
        case
    		when edit_date = '0' then NULL
    		else edit_date::date
    	end                                                   AS edit_date,
        upper(update_use::text)                               AS update_use,
        update_tim::date                                      AS update_time,
        length::double precision                              AS length,
        shape_len::double precision                           AS shape_len,
        geometry::GEOMETRY(GEOMETRY,4326)                     AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_street_center_lines') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
