{% macro set_fmc_mtd_fl_incr(dv_name, dag_name, proc_schema, proc_name) -%}
    {% set query -%}
        begin transaction;
        call "{{ proc_schema }}"."{{proc_name}}"(
            '{{ dag_name }}',
            {{dv_name}}_load_cycle_seq.nextval,
            TO_VARCHAR(TO_TIMESTAMP_NTZ(current_timestamp))
        );
        commit;
    {%- endset %}

    {% do run_query(query) %}

{% endmacro %}