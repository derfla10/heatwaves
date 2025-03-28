REM +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
REM + Program : sync_heatwaves.sql                            +
REM + Author  : Alfred de Jager                               +
REM + Creation: 2017 07 17                                    +
REM + Version : 1.0                                           +
REM + Purpose : Synchronize Heatwaves of GRID_025DD           +
REM +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SET SERVEROUTPUT ON SIZE UNLIMITED
prompt Starting GRID_025 Decimal Degrees Temperature Wave synchronization...
set linesize 200
declare cursor getlastdays is
select g2d_id, start_day, end_day, total_days, wave_type, temperature_extreme 
  from grid_025dd_temperature_waves
 where end_day between (select max(end_day) - 1 from grid_025dd_temperature_waves)
                   and sysdate - 2;
mylastdays getlastdays%rowtype;
cursor getcheck is
select rowid
      ,end_day
	  ,total_days
	  ,temperature_extreme
  from grid_025dd_temperature_waves@toespositorain
 where g2d_id    = mylastdays.g2d_id
   and start_day = mylastdays.start_day;
mycheck getcheck%rowtype;
myinserts number;
myupdates number;
BEGIN
myinserts := 0;
myupdates := 0;
open getlastdays;
loop
  fetch getlastdays into mylastdays;
  exit when getlastdays%NOTFOUND;
  open getcheck;
  fetch getcheck into mycheck;
  if (getcheck%NOTFOUND) then
      INSERT INTO grid_025dd_temperature_waves@toespositorain (g2d_id, start_day, end_day, total_days, wave_type, temperature_extreme) 
	       VALUES (mylastdays.g2d_id, mylastdays.start_day, mylastdays.end_day, mylastdays.total_days, mylastdays.wave_type, mylastdays.temperature_extreme);
	  myinserts := myinserts + 1;
	  COMMIT;
  else
      if (mylastdays.end_day != mycheck.end_day or mylastdays.total_days != mycheck.total_days or mylastdays.temperature_extreme != mycheck.temperature_extreme) then
		  dbms_output.put_line('Updating ID: ' || to_char(mylastdays.g2d_id) || ' starting day ' || to_char(mylastdays.start_day,'Day DD Month YYYY') || ' to duration of ' || to_char(mylastdays.total_days) || ' days.');
    	  UPDATE grid_025dd_temperature_waves@toespositorain 
		    set end_day = mylastdays.end_day
			   ,total_days = mylastdays.total_days
			   ,temperature_extreme = mylastdays.temperature_extreme
		  WHERE rowid = mycheck.rowid;
          COMMIT;
          myupdates := myupdates + 1;
	  end if;
  end if;
  close getcheck;
end loop;
close getlastdays;
dbms_output.put_line('Inserted ' || to_char(myinserts) || ' Updated ' || to_char(myupdates) || ' Records.');
END;
/
.

delete from grid_025dd_temperature_waves@toespositorain
 where total_days < 3 
   and end_day < sysdate - 7
;
commit;