SET SERVEROUTPUT ON SIZE UNLIMITED
SET linesize 200
SET FEEDBACK OFF
spool c:\dev\refresh.bat
declare cursor getmaxday is
select max(total_days) + 2 max_total_days
  from grid_025dd_temperature_waves
 where end_day between (select max(end_day) from grid_025dd_temperature_waves) - 1 and sysdate - 1
;
mymaxday getmaxday%rowtype;
BEGIN
  open getmaxday;
  fetch getmaxday into mymaxday;
  if (getmaxday%FOUND) then
      for i in 2 .. mymaxday.max_total_days
	   loop
          dbms_output.put_line('powershell -ExecutionPolicy RemoteSigned -File C:\DEV\generate.temperature.maps.ps1 url twaves_duration ' || to_char(sysdate - i,'YYYY') || ' ' ||  to_char(sysdate - i,'MM') || ' ' || to_char(sysdate - i,'DD') || ' 0');
	  end loop;
  end if; 
  close getmaxday;
END;
/
.
spool off
SET FEEDBACK ON
host c:\dev\refresh.bat