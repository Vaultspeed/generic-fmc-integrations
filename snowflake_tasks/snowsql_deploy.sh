#!/bin/bash
set -e

##################################################
## Variables - change these for you environment ##
##################################################

snowsql_conn=bryan
snowflake_warehouse=VAULTSPEED
task_schema=TASKER
default_schedule="USING CRON 0 0 1 1 * America/New_York" ## Here, 01 January at midnight is the default when no schedule is available in the FMC definition
agent_folder=/home/vsstudent/agent
zipname=$1

## Set file and directory paths
path_to_generated_files=`cat $agent_folder/client.properties | grep ^path.in | cut -d '=' -f 2`
# Check if the zipfile is there
if ! [ -f $path_to_generated_files/$zipname ]; then
    echo "cannot find file $path_to_generated_files/$zipname"
    echo "exiting script"
    exit 0
fi

## Get basename of zipfile (= remove everything after the first '.'), this will be used as the foldername to unzip the files in
dirname=$path_to_generated_files/${zipname%%.*}

## determine name of logfile
## logfile=$dirname"_deploy.log"
## echo "name of logfile: $logfile"

unzip -q -u $path_to_generated_files/$zipname -d $dirname

## Clean up the mapping file--remove backslash escape
fmc_json_mapping=$(cat $path_to_generated_files/${zipname%%.*}/*mappings*.json)
bkslshstr='\\"'
fmc_json_mapping=${fmc_json_mapping//$bkslshstr/}

## Truncate table that stores JSON mapping
snowsql -c $snowsql_conn -o exit_on_error=true -q "TRUNCATE TASKER.TASK_MAPPING;"

## Insert JSON mapping into task mapping table; use as work table for task generation procedure
snowsql -c $snowsql_conn -o exit_on_error=true -q "INSERT INTO TASKER.TASK_MAPPING (JSON_MAPPING) SELECT TO_VARIANT(PARSE_JSON('$fmc_json_mapping'));"

## Remove VaultSpeed ASCII header from info JSON
fmc_json_text=$(cat $path_to_generated_files/${zipname%%.*}/*FMC_info*.json)
dv_find_str='"dv_code"':
pos=$(awk -v a="$fmc_json_text" -v b="$dv_find_str" 'BEGIN{print index(a,b)}')
fmc_json_text={"${fmc_json_text:$pos+33}"

## Get DAG name and scheduling parameters from info JSON
dag_name=$(echo $fmc_json_text|jq -r '.dag_name')
schedule_interval=$(echo $fmc_json_text|jq -r '.schedule_interval')

##Add a default schedule if no value is available in schedule_interval (required for Snowflake Tasks)
if [ "$schedule_interval" == "" ] 
then
    schedule_interval=$default_schedule
fi

## Execute procedure to generate tasks/dag
snowsql -c $snowsql_conn -o exit_on_error=true -q "CALL TASKER.CREATE_VS_FMC('$snowflake_warehouse', '$task_schema', '$dag_name', '$schedule_interval');"


exit 0
