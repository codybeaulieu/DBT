-- depends_on: {{ ref('elt_model_statistics') }}
{# ms_getlatestrun('Schema.Table') Fetches the latest run date of the queried model as stored on post hook to ref/util table #}
{% macro ms_getlatestrun(tbl) %}
    {% if execute %}  
        {% if should_full_refresh() %}
            {%- set result = '1970-01-01' -%}  
        {% else %}    
            {%- call statement('GetDT', fetch_result=True) -%}
                SELECT
                    TO_CHAR(MAX_ELT_PROCESS_DTM) max_load_dt
                FROM {{ ref('elt_model_statistics') }}
                WHERE TABLE_SCHEMA = upper(split_part('{{tbl}}','.',2))
                    AND TABLE_NAME= upper(split_part('{{tbl}}','.',3))
                QUALIFY MAX(ELT_BATCH_DTM) OVER (PARTITION BY TABLE_SCHEMA,TABLE_NAME) = ELT_BATCH_DTM
                UNION SELECT '1970-01-01'
            {%- endcall -%}
            {%- set result = load_result('GetDT')['data'][0][0] -%}
        {% endif %}
        {{ return(result) }}
    {% endif %}    
{% endmacro %}

{# ms_insert('Schema.Table') Inserts Row Count and Max ELT Process DateTime of run and Batch Execution DT as STATS_DTS to Model Statistics #}

{% macro ms_insert(tbl) %}
    {% if execute %}  
        {%- call statement('Insert', fetch_result=False) -%}
            INSERT INTO {{ ref('elt_model_statistics') }}
                (TABLE_SCHEMA,TABLE_NAME,ELT_BATCH_DTM,ROW_COUNT,MAX_ELT_PROCESS_DTM)
            SELECT  
                upper(split_part('{{tbl}}','.',2))          AS TABLE_SCHEMA
                ,upper(split_part('{{tbl}}','.',3))         AS TABLE_NAME
                ,'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'::TIMESTAMP_NTZ(9)   AS ELT_BATCH_DTM
                ,COUNT(*)                                   AS ROW_COUNT
                ,MAX(ELT_PROCESS_DATETIME)                  AS MAX_ELT_PROCESS_DTM
            FROM {{tbl}}
            group by all
        {%- endcall -%}
    {% endif %}    
{% endmacro %}