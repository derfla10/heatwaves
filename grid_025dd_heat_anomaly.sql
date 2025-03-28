REM ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
REM + Program : grid_025dd_heat_anomaly.sql                          +
REM + Author  : Alfred de Jager                                      +
REM + Version : 1.0                                                  +
REM + Created : 19 August 2016                                       +
REM + Purpose : Calculation of temperature anomaly                   +
REM +           for previous day                                     +
REM + Change  :                                                      +
REM + Input   : Values in table GRID_025DD_HEAT                      +
REM + Output  : Values in table GRID_025DD_HEAT ABSORBED column      +
REM +           Update of Column Comments in GRID_025DD_HEAT         +
REM ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SET SERVEROUTPUT ON SIZE UNLIMITED
COLUMN temp_field     format A13  new_value atemp_field;
COLUMN anomaly_field  format A12  new_value aanomaly_field;
COLUMN comment_field  format A140 new_value acomment_field;
COLUMN status_field   format A100 new_value astatus_field;
set linesize 250
COLUMN year   format 9999 new_value myyear
COLUMN month  format a2   new_value mymonth
COLUMN day    format a2   new_value myday
SELECT '&1' year
      ,'&2' month
      ,'&3' day
FROM dual;
select 'temp_max_'     || '&mymonth' || '&myday' temp_field
      ,'anomaly_'      || '&mymonth' || '&myday' anomaly_field
      ,'Processed on ' || to_char(sysdate,'Day DD Month YYYY HH24:MI') || ' for year ' || '&myyear' || ' reference period is from 1981 to and including 2010, null value for less than 7 years.'  comment_field
FROM dual;

DECLARE
cursor get_temp_avg is
SELECT g2d_id                             id
      ,avg(&atemp_field)                  avg_temp
      ,stddev(&atemp_field)               std_temp
      ,sum(decode(&atemp_field,NULL,0,1)) total
 FROM grid_025dd_heat
WHERE year between 1981 and 2010
GROUP BY g2d_id
;
mytemp_avg get_temp_avg%rowtype;
cursor get_temp is
SELECT s.rowid
     , round((&atemp_field - mytemp_avg.avg_temp) / decode(mytemp_avg.std_temp,0,0.1,mytemp_avg.std_temp),1) temp_anomaly
  FROM grid_025dd_heat s
 WHERE g2d_id = mytemp_avg.id
   AND s.year = &myyear
;
mytemp get_temp%rowtype;
counter number;
BEGIN
counter := 0;
OPEN get_temp_avg;
LOOP 
    FETCH get_temp_avg INTO mytemp_avg;
    EXIT WHEN get_temp_avg%NOTFOUND;
    OPEN get_temp;
    FETCH get_temp INTO mytemp;
    if (mytemp_avg.total < 7) then
         UPDATE grid_025dd_heat
            SET &aanomaly_field = NULL
          WHERE rowid = mytemp.rowid AND &aanomaly_field is not null
         ;
         counter := counter + 1;
    elsif (mytemp.temp_anomaly > -9 and mytemp.temp_anomaly < 9) then
         UPDATE grid_025dd_heat
            SET &aanomaly_field = mytemp.temp_anomaly
          WHERE rowid = mytemp.rowid
         ;
         counter := counter + 1;
    end if;
    CLOSE get_temp;
    if ( mod(counter,10) = 0 ) then
       COMMIT;
    end if;
END LOOP;
CLOSE get_temp_avg;
dbms_output.put_line('Found: ' || to_char(counter) || ' records with values');
END;
/
.
COMMIT;
COMMENT ON COLUMN GRID_025DD_HEAT.&aanomaly_field  IS '&acomment_field'; 
prompt Ended grid_025dd_heat_anomaly.sql ...
select to_char(sysdate,'Day DD Month YYYY HH24 MI:ss') end_time from dual;


