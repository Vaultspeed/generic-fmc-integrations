# Prerequisites:
NOTE: This code package does not yet handle Object-specific loading window. You must use standard batch loading with parameter OBJECT_SPECIFIC_LOADING_WINDOW = N in VaultSpeed.

 - This code package assumes you have setup your VaultSpeed agent and it is able to connect to your Snowflake account and has privileges to run DDL scripts.

 - In your agent environment, install:
        - SnowSQL (Snowflake SQL API) https://docs.snowflake.com/en/user-guide/snowsql-install-config
        - jq (JSON tool for command line) https://jqlang.github.io/jq/download/

 - Copy the deployment script, snowsql_deploy.sh, to your agent folder.

 - In the agent's client.properties file, set the path of the deploy.cmd parameter to the path for snowsql_deploy.sh.
    Example line: 
        deploy.cmd = sh /home/agent/snowsql_deploy.sh {zipname}

 - Restart the VaultSpeed agent for the new settings to take effect.

 - In the SnowSQL config file (~/.snowsql/config), name the connection and set the connection properties to be used by the SnowSQL API and save it. You can use your preferred authentication method. This example uses USERNAME and PASSWORD authentication:
        
        [connections.{your_connection_name}]
        #Can be used in SnowSql as #connect example

        accountname = {your_account}.{your_region}.{your_cloud}
        username = {your_username}
        password = {your_password}
        warehousename = {your_warehouse}
        dbname = {your_db}

 - Open the snowsql_deploy.sh shell script, and the change the variable values to reflect your environments, and save it:

        snowsql_conn= {your_connection_name}
        snowflake_warehouse= {your_warehouse}
        task_schema= {your_Snowflake_task_schema}
        default_schedule= "{your_default_schedule}"
        agent_folder= {your_agent_folder_path}

 - Run the script SF_DAG_DEPLOY_OBJECTS.sql in your target Snowflake database.

 - In VaultSpeed, set up a Data Vault with FMC_TYPE = generic and ETL Generation type = Snowflake SQL.
 
 - Generate the DDL and ETL for your Data Vault and deploy both to the target database.
 
 - Generate the FMC workflows. Be sure to use a valid schedule interval provided in the Snowflake Tasks documentation. Do not enclose the schedule interval value in double quotes (") or single quotes ('). Make sure Group Tasks is toggled off. (VaultSpeed will require a numeric Concurrency value, but it will not impact the workflow.
 
# Usage
After completing the prerequisites, go the Automatic Deployment screen in your VaultSpeed subscription. Select the generic FMC generation you wish to deploy and click the up arrow to deploy it. Select the Custom Script option and click deploy.