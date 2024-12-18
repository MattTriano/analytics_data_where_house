{%- macro get_and_add_new_and_updated_records_to_data_raw(
    dataset_name,
    source_cols,
    metadata_cols=["source_data_updated", "ingestion_check_time"]
) -%}
    {{ config(materialized='table') }}
    {%- set source_ref = source('data_raw', dataset_name) -%}
    {%- set temp_source_ref = source('data_raw', 'temp_' ~ dataset_name) -%}
    {%- set temp_table_name = 'temp_' ~ dataset_name -%}

    {# Check if the temp table exists #}
    {%- set temp_source_exists =
        dbt_utils.get_relations_by_pattern(
            schema_pattern='data_raw',
            table_pattern=temp_table_name
        ) | length > 0
    -%}

    {# selecting all records already in the full data_raw table #}
    WITH records_in_data_raw_table AS (
        SELECT *, 1 AS retention_priority
        FROM {{ source_ref }}
    )

    {%- if temp_source_exists -%}
        , current_pull_with_distinct_combos_numbered AS (
            SELECT *,
                row_number() over(partition by
                    {% for sc in source_cols %}{{ sc }},{% endfor %}
                    {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
                ) as rn
            FROM {{ source('data_raw', temp_table_name) }}
        )
        , distinct_records_in_current_pull AS (
            SELECT
                {% for sc in source_cols %}{{ sc }},{% endfor %}
                {% for mc in metadata_cols %}{{ mc }},{% endfor %}
                2 AS retention_priority
            FROM current_pull_with_distinct_combos_numbered
            WHERE rn = 1
        )
        , data_raw_table_with_all_new_and_updated_records AS (
            SELECT *
            FROM records_in_data_raw_table
            UNION ALL
            SELECT *
            FROM distinct_records_in_current_pull
        )
    {%- else -%}
        , data_raw_table_with_all_new_and_updated_records AS (
            SELECT *
            FROM records_in_data_raw_table
        )
    {%- endif -%}

    {# selecting records that where source columns are distinct (keeping the earlier recovery #}
    {#  when there are duplicates to chose from) #}
    , data_raw_table_with_new_and_updated_records AS (
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
{%- endmacro -%}