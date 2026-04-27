{% macro set_fmc_mtd_bv_incr(dv_name, dag_name, proc_schema, proc_name) -%}
    {% set query -%}
        begin transaction;
        call "{{ proc_schema }}"."{{proc_name}}"(
            '{{ dag_name }}',
            {{dv_name}}_load_cycle_seq.nextval,
            TO_VARCHAR(TO_TIMESTAMP_NTZ(current_timestamp))
        );
        commit;
    {%- endset %}

    {% set wait_query -%}
        call wait_for_running_flows();
    {%- endset %}

    {# This will wait for all other flows to finish, only after it ends can we start the BV loading process #}
    {% do run_query(wait_query) %}

    {% do run_query(query) %}

{% endmacro %}