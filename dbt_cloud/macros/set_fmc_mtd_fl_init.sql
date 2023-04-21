{% macro set_fmc_mtd_fl_init(dv_name, dag_name, proc_schema, proc_name, start_date) -%}
    {%- set lci_table = dag_name + '_lci' -%}
    {% set query -%}
        begin transaction;
        drop table if exists {{lci_table}};
        create table {{lci_table}} as select {{dv_name}}_load_cycle_seq.nextval as lci;
        call "{{ proc_schema }}"."{{proc_name}}"(
            '{{ dag_name }}',
            (select lci from {{lci_table}}),
            '{{ start_date }}'
        );
        commit;
    {%- endset %}

    {% do run_query(query) %}

{% endmacro %}