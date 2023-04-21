
CREATE SEQUENCE MOTO_LOAD_CYCLE_SEQ;

CREATE OR REPLACE PROCEDURE wait_for_running_flows()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
  LET running := 0;
  REPEAT
    CALL SYSTEM$WAIT(10);
    SELECT count(1) INTO :running FROM "moto_fmc"."fmc_loading_history" WHERE "success_flag" = 0;
  UNTIL (running = 0)
  END REPEAT;
  RETURN 'All flows are finished';
END;
