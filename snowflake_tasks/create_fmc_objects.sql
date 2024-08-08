USE DATABASE {YOUR_DB};

CREATE OR REPLACE SCHEMA TASKER;

CREATE OR REPLACE STAGE TASK_JSON_STAGE FILE_FORMAT = (TYPE = 'JSON');

CREATE OR REPLACE TABLE TASKER.TASK_MAPPING (JSON_MAPPING VARIANT );

CREATE OR REPLACE SEQUENCE TASKER.FMC_SEQ START = 1 INCREMENT = 1;

CREATE OR REPLACE PROCEDURE TASKER.GET_LOAD_CYCLE_ID()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
      var LoadCycleIdRslt=snowflake.execute({ sqlText: " SELECT TASKER.FMC_SEQ.NEXTVAL"});
      LoadCycleIdRslt.next();
      var LoadCycleId = LoadCycleIdRslt.getColumnValue(1);
      var lci_str="''"+LoadCycleId+"''";
      var sql_str="CALL SYSTEM$SET_RETURN_VALUE("+lci_str+")";
      var res = snowflake.execute({sqlText:sql_str});
      return "SUCCESS";
';

CREATE OR REPLACE PROCEDURE TASKER.RUN_MAPPING_PROC("PROC_NAME" VARCHAR(50), "LOAD_CYCLE_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var call_sp_sql = "CALL "+PROC_NAME;
    try {
        snowflake.execute( {sqlText: call_sp_sql} );
        result = "SUCCESS";
        }
    catch (err)  {
        result =  "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
        result += "\\\\n  Message: " + err.message;
        result += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;
        }
    return result;
';

CREATE OR REPLACE PROCEDURE TASKER.CREATE_VS_FMC("VAR_WH" VARCHAR(255), "TASK_SCHEMA" VARCHAR(255), "DAG_NAME" VARCHAR(240), "LOAD_SCHED" VARCHAR(100), "P_LOAD_DATE" VARCHAR(100), "LOAD_TYPE" VARCHAR(10), "SET_EXPLICIT_LOAD_DATE" VARCHAR(10))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE var_job VARCHAR(255) default '''';            -- assigned task name derived from procedure name
        var_proc VARCHAR(255)default '''';            -- task stored procedure name
        var_dep VARCHAR(255) default '''';            -- task dependency name
        var_seq INT default 0;                        -- load group sequence number
        dep_arr ARRAY default ARRAY_CONSTRUCT();      -- array for dependencies
        dep_counter INTEGER default 0;                -- dependency counter
        dep_list VARCHAR(10000) default '''';         -- dependency list for AFTER clause in CREATE TASK statement
        dep_last_task VARCHAR(255) default '''';      -- last dependency (needed to pass load cycle id to last task in dag)
        proc_schema VARCHAR(255) default '''';        -- schema containing load procedures
        task_name VARCHAR(255) default '''';          -- name of the task in the TASK CREATE statement
        task_sql VARCHAR(10000) default '''';         -- single create task sql statement
        var_sql VARCHAR(150000) default '''';         -- string containing sql statement to be executed
        max_layer INT default 0;                      -- Number of load layers in FMC workflow
        task_counter INT default 0;                   -- counter tallying # of tasks
        load_date VARCHAR(100);                       -- LOAD_DATE value
        dep_grp_prefix VARCHAR(10) default ''DEP_GRP_''; -- prefix for dependency groups
        
BEGIN
    -- Create initial task for getting load cycle id
    var_sql := ''CREATE OR REPLACE TASK EXEC_''||UPPER(dag_name)||'' WAREHOUSE = ''||var_wh||'' SCHEDULE = ''''''||load_sched||'''''' AS CALL ''||UPPER(task_schema)||''.GET_LOAD_CYCLE_ID();'';
    EXECUTE IMMEDIATE var_sql;
    task_counter := task_counter + 1;

    -- Create temp table for task mappings (jobs)
    CREATE OR REPLACE TEMP TABLE JOBS (SEQ INT, JOB VARCHAR(255), PROC_SCHEMA VARCHAR(255), DEPENDENCY VARCHAR(255));

    -- Insert task mappings into JSON temp table, assigning a number sequence to each load layer.
    INSERT INTO JOBS (SEQ, JOB, PROC_SCHEMA, DEPENDENCY)
    WITH JD AS (
    SELECT j.KEY AS JOB
        ,j.VALUE:map_schema::string AS PROC_SCHEMA
        ,d.VALUE::string AS DEPENDENCY
    FROM TASK_MAPPING,
        LATERAL FLATTEN(input=> JSON_MAPPING) j, 
        LATERAL FLATTEN(input=> j.VALUE:dependencies, outer=> true) d
    )
    ,"00" AS (
        SELECT 0 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY is null
     )
    ,"01" AS (
        SELECT 1 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY IN (SELECT JOB FROM "00")
    )
    ,"02" AS (
        SELECT 2 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY IN (SELECT JOB FROM "01")
    )
    ,"03" AS (
        SELECT 3 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY IN (SELECT JOB FROM "02")
    )
    ,"04" AS (
        SELECT 4 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY IN (SELECT JOB FROM "03")
    )
    ,"05" AS (
        SELECT 5 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY IN (SELECT JOB FROM "04")
    )
    ,"06" AS (
        SELECT 6 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY IN (SELECT JOB FROM "05")
    )
    ,"07" AS (
        SELECT 7 AS SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM JD WHERE DEPENDENCY IN (SELECT JOB FROM "06")
    )
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "00"
    UNION ALL
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "01"
    UNION ALL
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "02"
    UNION ALL
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "03"
    UNION ALL
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "04"
    UNION ALL
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "05"
    UNION ALL
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "06"
    UNION ALL
    SELECT SEQ, JOB, PROC_SCHEMA, DEPENDENCY FROM "07";

    -- Get the number of load layers
    SELECT MAX(SEQ) INTO max_layer FROM JOBS;

    -- Conditional logic to set the LOAD DATE value
    load_date := CASE WHEN :load_type = ''INIT'' AND :set_explicit_load_date = ''Y'' 
        THEN ''''''''||TO_VARCHAR(P_LOAD_DATE::timestamp)||''''''''
        ELSE ''CURRENT_TIMESTAMP''
    END;
    
    -- Create task for logging start of FMC
    SELECT JOB INTO var_job FROM JOBS WHERE SEQ = 0;
    SELECT PROC_SCHEMA INTO proc_schema FROM JOBS WHERE SEQ = 0;
    var_sql := ''CREATE OR REPLACE TASK EXEC_''||UPPER(var_job)||'' WAREHOUSE = ''||var_wh||'' AFTER EXEC_''||UPPER(dag_name)||'' AS 
    BEGIN
        CALL ''||UPPER(proc_schema)||''.''||UPPER(var_job)||''(''''''||dag_name||'''''', TO_VARCHAR(SYSTEM$GET_PREDECESSOR_RETURN_VALUE()), TO_VARCHAR(''||load_date||''::timestamp));
        CALL SYSTEM$SET_RETURN_VALUE(SYSTEM$GET_PREDECESSOR_RETURN_VALUE());
    END;'';
    EXECUTE IMMEDIATE var_sql;
    task_counter := task_counter + 1;

    -- Create load tasks in order of dependency for all mappings after the initial mapping (>0). 
    -- The MAX() on SEQ is applied to insure procedures that span multiple layers run after all its dependent tasks have been created.
    DECLARE j1 cursor for SELECT JOB, PROC_SCHEMA, MAX(SEQ) AS SEQ FROM JOBS WHERE SEQ > 0 GROUP BY JOB, PROC_SCHEMA ORDER BY SEQ;
    BEGIN
        FOR record in j1 do
            var_proc := record.JOB;
            proc_schema := record.PROC_SCHEMA;
            var_seq := record.SEQ;
            
            -- set task name; if it is last task in the dag add the dag name to the end logging procedure
            task_name := CASE WHEN :var_seq = :max_layer THEN 
                                    ''EXEC_''||UPPER(var_proc)||''_''||dag_name
                                ELSE 
                                    ''EXEC_''||UPPER(var_proc)
                                END;
            
            task_sql := ''CREATE OR REPLACE TASK ''||task_name||'' WAREHOUSE = ''||var_wh||'' AFTER '';

            -- Build dependency list; 
            SELECT ARRAY_AGG(TO_ARRAY(DEPENDENCY)) INTO dep_arr FROM JOBS WHERE JOB = :var_proc; 
            dep_list := '''';
            dep_counter := 0;
            FOR dep in dep_arr DO
                dep_counter := dep_counter + 1;
                dep_list := dep_list||CASE WHEN :dep_counter > 1 THEN '', '' ELSE '''' END||''EXEC_''||UPPER(ARRAY_TO_STRING(dep,'', ''));
                --Get the last dependency, ensuring only one dependency task is called to fetch the load cycle id.
                dep_last_task := ''EXEC_''||UPPER(ARRAY_TO_STRING(dep,'', ''));
            END FOR;

            -- Build create task statements; the last layer is run without the handling procedure.
            var_sql := CASE WHEN :var_seq = :max_layer THEN  -- last task in DAG; SUCCESS
                                task_sql||dep_list||'' AS CALL ''||UPPER(proc_schema)||''.''||UPPER(var_proc)||''(TO_VARCHAR(SYSTEM$GET_PREDECESSOR_RETURN_VALUE(''''''||dep_last_task||'''''')), ''''1'''');''
                            -- If a Dependency Group, just pass through load cycle id, otherwise run load procedure within handling procedure
                            WHEN LEFT(:var_proc, 8) = :dep_grp_prefix THEN
                                task_sql||dep_list||'' AS CALL SYSTEM$SET_RETURN_VALUE(SYSTEM$GET_PREDECESSOR_RETURN_VALUE(''''''||dep_last_task||''''''));''       
                            ELSE 
                                task_sql||dep_list||'' AS BEGIN 
                                                            CALL ''||UPPER(task_schema)||''.RUN_MAPPING_PROC(''''''||UPPER(proc_schema)||''.''||UPPER(var_proc)||''()'''', TO_VARCHAR(SYSTEM$GET_PREDECESSOR_RETURN_VALUE(''''''||dep_last_task||''''''))); 
                                                            CALL SYSTEM$SET_RETURN_VALUE(SYSTEM$GET_PREDECESSOR_RETURN_VALUE(''''''||dep_last_task||'''''')); 
                                                          END;''
                       END;
            EXECUTE IMMEDIATE var_sql;
            task_counter := task_counter + 1;
        END FOR;
    END;

    DROP TABLE JOBS;

    -- Enable all tasks in the dag
    SELECT SYSTEM$TASK_DEPENDENTS_ENABLE(''EXEC_''||:dag_name);

    RETURN TO_VARCHAR(task_counter)||'' tasks created for dag ''''''||''EXEC_''||:dag_name||''''''.'';

END;
';
