{% macro generate_clean_stage_incremental_dedupe_query(
    dataset_name,
    record_id,
    ck_cols=None,
    base_cols=None,
    updated_at_col='source_data_updated'
) %}
    {% do log("ck_cols: " ~ ck_cols, info=True) %}
    {% do log("base_cols: " ~ base_cols, info=True) %}
    {% do log("dataset_name: " ~ dataset_name, info=True) %}
    {{
        config(
            materialized='incremental',
            unique_key=record_id,
            incremental_strategy='merge',
            on_schema_change='sync_all_columns'
        )
    }}
    {% if ck_cols is none %}
        {% set ck_cols = [] %}
    {% endif %}
    {% if base_cols is none %}
        {% set base_cols = [] %}
    {% endif %}

    {% if not dataset_name.endswith('_standardized') %}
        {% set dataset_name = dataset_name ~ '_standardized' %}
    {% endif %}
    {%- set source_ref = ref(dataset_name) -%}

    {% do log("ck_cols after conditionals: " ~ ck_cols, info=True) %}
    {% do log("base_cols after conditionals: " ~ base_cols, info=True) %}
    {% do log("dataset_name after conditionals: " ~ dataset_name, info=True) %}
    {% do log("source_ref after conditionals: " ~ source_ref, info=True) %}

    -- selects all records from the standardized view of this data
    WITH std_data AS (
        SELECT *,
            md5(
                {%- if base_cols|length > 0 -%}
                concat(
                    {%- for col in base_cols -%}
                        coalesce({{ col }}::varchar, '')
                        {%- if not loop.last %}, '_', {% endif -%}
                    {%- endfor -%}
                )
                {%- else -%}
                ''
                {%- endif -%}
            ) AS record_hash
        FROM {{ source_ref }}
        {% if is_incremental() %}
        WHERE {{ record_id }} IN (
            -- Get IDs from new/updated records
            SELECT {{ record_id }}
            FROM {{ source_ref }}
            WHERE {{ updated_at_col }} > (SELECT max({{ updated_at_col }}) FROM {{ this }})
        )
        {% endif %}
    ),

    -- keeps the most recently updated version of each record
    latest_records AS (
        SELECT *,
            row_number() over(partition by {{ record_id }} ORDER BY {{ updated_at_col }} DESC) as rn
        FROM std_data
    ),
    new_or_updated_records AS (
        SELECT * FROM latest_records WHERE rn = 1
    ),

    {% if is_incremental() %}
    existing_versions AS (
        SELECT record_hash, min({{ updated_at_col }}) as version_first_seen
        FROM {{ this }}
        GROUP BY record_hash
    ),
    {% endif %}

    version_first_seen AS (
        {% if is_incremental() %}
        SELECT
            f.{{ record_id }},
            COALESCE(e.version_first_seen, f.{{ updated_at_col }}) as version_first_seen
        FROM new_or_updated_records f
        LEFT JOIN existing_versions e
        ON f.record_hash = e.record_hash
        {% else %}
        SELECT
            {{ record_id }},
            {{ updated_at_col }} as version_first_seen
        FROM new_or_updated_records
        {% endif %}
    )

    SELECT
        {% if base_cols|length > 0 -%}
            {% for bc in base_cols -%}
                fnr.{{ bc }},
            {%- endfor %}
        {%- endif %}
        vfs.version_first_seen as first_ingested_pub_date,
        fnr.record_hash
    FROM new_or_updated_records fnr
    JOIN version_first_seen vfs
    ON fnr.{{ record_id }} = vfs.{{ record_id }}
    ORDER BY
        {% if ck_cols|length > 0 -%}
            {% for ck in ck_cols %}fnr.{{ ck }} DESC, {% endfor %}
        {%- endif -%}
        fnr.{{ updated_at_col }} DESC
{% endmacro %}