{% macro generate_clean_stage_incremental_dedupe_query(
    dataset_name,
    record_id,
    ck_cols=None,
    base_cols=None,
    updated_at_col='source_data_updated'
) %}
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

    -- selects all records from the standardized view of this data
    WITH std_data AS (
        SELECT *
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
    )

    SELECT
        {% if base_cols|length > 0 -%}
            {% for bc in base_cols -%}
                fnr.{{ bc }}{{ "," if not loop.last }}
            {%- endfor %}
        {%- endif %}
    FROM new_or_updated_records fnr
    ORDER BY
        {% if ck_cols|length > 0 -%}
            {% for ck in ck_cols %}fnr.{{ ck }} DESC, {% endfor %}
        {%- endif -%}
        fnr.{{ updated_at_col }} DESC
{% endmacro %}