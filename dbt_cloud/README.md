# Prerequisites:
 - In VaultSpeed, set up a Data Vault with FMC_TYPE = Generic and ETL Generation type = Snowflake dbt and make sure you have all the FMC flows created
 - make sure to set an appropriate schedule interval for the FMC flows in VaultSpeed,
   for example: `{ "date": { "type": "every_day" }, "time": { "type": "every_hour", "interval": 1, } }`
 - Add to the dbt project YAML file the following line: `on-run-end: "{{ fmc_set_status(results) }}"`
 - Add the `set_fmc_mtd_fl_init.sql`, `set_fmc_mtd_fl_incr.sql`, `set_fmc_mtd_bv_init.sql`, `set_fmc_mtd_bv_incr.sql` and `fmc_upd_run_status_fl.sql` macros to your dbt project
 - Modify the content of `snowflake_fmc_prerequisites.sql` to match your settings (schema and object names) and deploy it to Snowflake.
   make sure to run it with the schema set as the same default as in dbt (e.g. the FMC schema)
 - Set the following environment variables: `dbt_account_id`;`dbt_environment_id`;`dbt_project_id`;`dbt_token`
 
# Usage
After completing the prerequisites, run the `dbt_cloud_fmc.py` script with as the argument the path to the directory containing the generated files.
The script will parse the Generic FMC JSON files in the FMC subdirectory of the provided path.
The result of the script is that an extra macro will be added to the macros subdirectory of the provided path, 
as well as the creation of multiple Jobs in the dbt Cloud instance provided by the environment variables.
