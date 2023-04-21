{% macro fmc_upd_run_status_fl(dag_name, proc_schema, proc_name, success_flag) -%}
    {%- set lci_table = dag_name + '_lci' -%}
    {% set query -%}
        begin transaction;
        call "{{ proc_schema }}"."{{proc_name}}"(
            (select lci from {{lci_table}}),
            '{{success_flag}}'
        );
        commit;
    {%- endset %}

    {% do run_query(query) %}

{% endmacro %}

{% macro fmc_upd_run_status_fl_object_based(dag_name, proc_schema, proc_name, success_flag) -%}
    {%- set lci_table = dag_name + '_lci' -%}
    {% set query -%}
        begin transaction;
        call "{{ proc_schema }}"."{{proc_name}}"(
            (select lci from {{lci_table}}),
            '{{success_flag}}',
            'Y'
        );
        commit;
    {%- endset %}

    {% do run_query(query) %}

{% endmacro %}