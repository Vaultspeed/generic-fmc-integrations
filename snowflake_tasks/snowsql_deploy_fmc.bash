#!/bin/bash
set -e

zipname=$1

###################################################
## Variables - change these for your environment ##
###################################################

## BEGIN EDIT ##
snowsql_conn=ACCOUNT_ADMIN
snowflake_warehouse=VAULTSPEED_WH
agent_folder=/home/azureuser/agent
task_schema=TASKER
default_schedule="USING CRON 0 0 1 1 * America/New_York" ## Here, 01 January at midnight is the default when no schedule is available in the FMC definition
use_start_dt_as_load_dt=Y #Sets load date for incremental loads to the start date selected when creating FMC workflow
max_dep_per_grp=100 #Maximum number of dependencies per group
snowsql_dir=/home/azureuser/bin/
snowsql_config_path=/home/azureuser/.snowsql/config
task_stage="@TASK_JSON_STAGE"
## END EDIT ##

## Define the split_deps function
split_deps() {
    local json="$1"
    local max_deps_per_grp=$2

    # Get the max dependencies
    local total_deps=$(echo "$json" | jq '[.[].dependencies | length] | max')

    # Find the node with the most dependencies
    local node_to_split=$(echo "$json" | jq -r --argjson total_deps "$total_deps" 'to_entries[] | select(.value.dependencies | length == $total_deps) | .key')

    # Calculate the number of groups needed
    local num_groups=$(( (total_deps + max_deps_per_grp - 1) / max_deps_per_grp ))

    # Initialize an empty object for the new groups
    local new_groups='{}'

    # Calculate number of dependencies per group (as evenly as possible)
    local deps_per_group=$(( total_deps / num_groups ))
    local extra_deps=$(( total_deps % num_groups ))

    # Create each DEP_GRP_# dynamically and add to the new_groups object
    local start=0
    for (( i=1; i<=num_groups; i++ )); do
        local group_deps=$deps_per_group
        # Add one extra dependency to the first 'extra_deps' groups
        if (( i <= extra_deps )); then
            group_deps=$((group_deps + 1))
        fi
        local ending=$(( start + group_deps ))

        local group_name="DEP_GRP_$i"
        new_groups=$(echo "$new_groups" | jq --arg group_name "$group_name" --argjson start "$start" --argjson ending "$ending" --argjson deps "$json" --arg node_to_split "$node_to_split" '.[$group_name] = { "dependencies": ($deps | .[$node_to_split].dependencies[$start:$ending]) }')

        # Update start for the next group
        start=$ending
    done

    # Reconstruct the JSON structure with the new groups inserted before $node_to_split
    json=$(echo "$json" | jq --argjson new_groups "$new_groups" --arg node_to_split "$node_to_split" '. as $original | reduce (keys_unsorted[] | select(. != $node_to_split)) as $key ({}; .[$key] = $original[$key]) + $new_groups + {($node_to_split): $original[$node_to_split]}')

    local new_deps=$(jq -n --argjson num_groups "$num_groups" '[range(1; $num_groups+1) | "DEP_GRP_" + (tostring)]')

    json=$(echo "$json" | jq --argjson new_deps "$new_deps" --arg node_to_split "$node_to_split" '.[$node_to_split].dependencies = $new_deps')

    echo $json
}

## Set file and directory paths
path_to_generated_files=`cat $agent_folder/client.properties | grep ^path.in | cut -d '=' -f 2 | tr -d ' '`
## Check if the zipfile is there
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

## Remove VaultSpeed ASCII header from info JSON
fmc_json_text=$(cat $path_to_generated_files/${zipname%%.*}/*FMC_info*.json)
dv_find_str='"dv_code"':
pos=$(awk -v a="$fmc_json_text" -v b="$dv_find_str" 'BEGIN{print index(a,b)}')
fmc_json_text={"${fmc_json_text:$pos+33}"

## Get max number of dependencies and split the dependencies on the node with the max count
max_dep_ct=$(echo "$fmc_json_mapping" | jq '[.[].dependencies | length] | max')
if (( $max_dep_ct > $max_dep_per_grp )); then
    fmc_json_mapping=$(split_deps "$fmc_json_mapping" $max_dep_per_grp)
fi

## Build fmc mapping JSON filepath
fmc_json_filepath=$path_to_generated_files/${zipname%%.*}/fmc_mapping.json

## Echo out JSON mapping to file
echo "$fmc_json_mapping" > "$fmc_json_filepath"

## Truncate table that stores JSON mapping
${snowsql_dir}snowsql -c $snowsql_conn --config $snowsql_config_path -o exit_on_error=true -q "TRUNCATE TASKER.TASK_MAPPING;"

## Load JSON mapping file to internal stage in Snowflake
${snowsql_dir}snowsql -c $snowsql_conn --config $snowsql_config_path -o exit_on_error=true -q "PUT file://$fmc_json_filepath $task_stage OVERWRITE=TRUE;"

## Copy JSON mapping into task mapping table from internal stage; use JSON variant column from taks mapping table in task generation procedure
${snowsql_dir}snowsql -c $snowsql_conn --config $snowsql_config_path -o exit_on_error=true -q "COPY INTO TASKER.TASK_MAPPING FROM @TASK_JSON_STAGE/fmc_mapping.json FILE_FORMAT = (TYPE = 'JSON');"

## Get DAG name, schedule interval, group tasks setting, and target database type from info JSON
dag_name=$(echo $fmc_json_text|jq -r '.dag_name')
schedule_interval=$(echo $fmc_json_text|jq -r '.schedule_interval')
load_type=$(echo $fmc_json_text|jq -r '.load_type')
start_date=$(echo $fmc_json_text|jq -r '.start_date')
flow_type=$(echo $fmc_json_text|jq -r '.flow_type')
group_tasks=$(echo $fmc_json_text|jq -r '.group_tasks')
dv_database_type=$(echo $fmc_json_text|jq -r '.dv_database_type')

##Check for Grouped tasks
if [ "$group_tasks" == "true" ] 
then
    echo "Group tasks must be set to 'false' for Snowflake Tasks deployed with generic FMC." >&2
    exit 1
fi

##Check target database type
if [ "$dv_database_type" != "SNOWFLAKE" ] 
then
    echo "The data vault database type must 'Snowflake' for Snowflake Tasks deployed with generic FMC." >&2
    exit 2
fi

##Add a default schedule if no value is available in schedule_interval (required for Snowflake Tasks)
if [ "$schedule_interval" == "" ] 
then
    schedule_interval=$default_schedule
fi

## Execute procedure to generate tasks/dag
${snowsql_dir}snowsql -c $snowsql_conn --config $snowsql_config_path -o exit_on_error=true -q "CALL TASKER.CREATE_VS_FMC('$snowflake_warehouse', '$task_schema', '$dag_name', '$schedule_interval', '$start_date', '$load_type', '$use_start_dt_as_load_dt');"


exit 0
