{% macro set_fmc_mtd_fl_init(dv_name, dag_name, proc_schema, proc_name, start_date) -%}
    {% set query -%}
        begin transaction;
        call "{{ proc_schema }}"."{{proc_name}}"(
            '{{ dag_name }}',
            {{dv_name}}_load_cycle_seq.nextval,
            '{{ start_date }}'
        );
        commit;
    {%- endset %}

    {% do run_query(query) %}

{% endmacro %}