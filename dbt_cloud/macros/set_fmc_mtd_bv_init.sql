{% macro set_fmc_mtd_bv_init(dv_name, dag_name, proc_schema, proc_name, start_date) -%}
    {% set query -%}
        begin transaction;
        call "{{ proc_schema }}"."{{proc_name}}"(
            '{{ dag_name }}',
            {{dv_name}}_load_cycle_seq.nextval,
            '{{ start_date }}'
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