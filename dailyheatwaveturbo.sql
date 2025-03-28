SET SERVEROUTPUT ON SIZE UNLIMITED

column ayear format 9999 new_value year
column ammdd format a4   new_value mmdd
set linesize 100
prompt Starting Daily Temperature Wave Computation...
select &1   ayear
      ,'&2' ammdd
      ,to_char(sysdate,'Day DD Month YYYY HH24:MI') start_time
  from dual;

declare cursor getdata is
select g2d_id
      ,temp_ten_&mmdd      ten
	  ,temp_ninety_&mmdd   ninety 
  from grid_025dd_heat_thresholds order by g2d_id
;
mydata getdata%rowtype;

cursor getmaxdata is
select temp_max_&mmdd    temperature
  from grid_025dd_heat
 where g2d_id = mydata.g2d_id
   and year = &year
;
mymaxdata getmaxdata%rowtype;

cursor getmindata is
select c.temp_min_&mmdd    temperature
      ,t.temp_ten_&mmdd    ten
	  ,t.temp_ninety_&mmdd ninety 
  from grid_025dd_cold c
      ,grid_025dd_cold_thresholds t
 where c.g2d_id = t.g2d_id
   and c.g2d_id = mydata.g2d_id
   and c.year = &year
;
mymindata getmindata%rowtype;
mytemp number;
cursor getwaves is
select rowid
      ,temperature_extreme
  from grid_025dd_temperature_waves
 where g2d_id  = mydata.g2d_id
   and ( end_day = to_date(&year || '&mmdd','YYYYMMDD') - 1
    or   end_day = to_date(&year || '&mmdd','YYYYMMDD') - 2 )
;
mywave getwaves%rowtype;
mycounter     number(6);
mywavecounter number(6);
BEGIN
update heatprd.messages set amessage = 'Starting Heatwave';
mycounter     := 0;
mywavecounter := 0;
commit;
open getdata;
loop
  fetch getdata into mydata;
  exit when getdata%NOTFOUND;
  mycounter := mycounter + 1;
  update heatprd.messages set amessage = to_char(mywavecounter) || ' waves of ' || to_char(mycounter);
  commit;
  open getmaxdata;
  fetch getmaxdata into mymaxdata;
  if (getmaxdata%FOUND) then
    if (mymaxdata.temperature > mydata.ninety) then
     -- max temperature heatwave
	 open getmindata;
	 fetch getmindata into mymindata;
	 if (getmindata%FOUND) then
	     if (mymindata.temperature > mymindata.ninety) then
		   -- min temperature heatwave 
		   mytemp := mymaxdata.temperature;
		   mywavecounter := mywavecounter + 1;
		   open getwaves;
		   fetch getwaves into mywave;
		   if (getwaves%FOUND) then
		     if (mywave.temperature_extreme > mytemp) then
			     mytemp := mywave.temperature_extreme;
		     end if;
             dbms_output.put_line('Settting end day to ' || &year || &mmdd || ' for Heatwave cell : ' || to_char(mydata.g2d_id));
		     update grid_025dd_temperature_waves set end_day = to_date(&year || '&mmdd','YYYYMMDD') , total_days = total_days + 1, temperature_extreme = mytemp where rowid = mywave.rowid;
		   else
             dbms_output.put_line('Inserting Heatwave for ' || &year || &mmdd || ' for cell : ' || to_char(mydata.g2d_id));
		     insert into grid_025dd_temperature_waves (g2d_id, start_day, end_day, total_days, wave_type, temperature_extreme) 
		          values (mydata.g2d_id,to_date(&year || '&mmdd','YYYYMMDD'),to_date(&year || '&mmdd','YYYYMMDD'),1,'H',mytemp);
		   end if;
		   close getwaves;
		   commit;
		end if;
	 end if;
	 close getmindata;
     elsif (mymaxdata.temperature < mydata.ten) then
     -- max temperature coldwave
	   open getmindata;
	   fetch getmindata into mymindata;
	   if (getmindata%FOUND) then
	    if (mymindata.temperature < mymindata.ten) then
		   -- min temperature coldwave 
		   mytemp := mymindata.temperature;
		   mywavecounter := mywavecounter + 1;
		   open getwaves;
		   fetch getwaves into mywave;
		   if (getwaves%FOUND) then
		     if (mywave.temperature_extreme < mytemp) then
			     mytemp := mywave.temperature_extreme;
		     end if;
             dbms_output.put_line('Settting end day to ' || &year || &mmdd || ' for Coldwave cell : ' || to_char(mydata.g2d_id));
		     update grid_025dd_temperature_waves set end_day = to_date(&year || '&mmdd','YYYYMMDD') , total_days = total_days + 1, temperature_extreme = mytemp where rowid = mywave.rowid;
		   else
             dbms_output.put_line('Inserting Coldwave for ' || &year || &mmdd || ' for cell : ' || to_char(mydata.g2d_id));
		     insert into grid_025dd_temperature_waves (g2d_id, start_day, end_day, total_days, wave_type, temperature_extreme) 
		          values (mydata.g2d_id,to_date(&year || '&mmdd','YYYYMMDD'),to_date(&year || '&mmdd','YYYYMMDD'),1,'C', mytemp);
		   end if;
		   close getwaves;
		   commit;
		end if;
	   end if;
	   close getmindata;
     end if;
  end if;
  close getmaxdata;
end loop;
close getdata;
END;
/
.

prompt Ending Daily Temperature Wave computation.
select to_char(sysdate,'Day DD Month YYYY HH24:MI') end_time
  from dual;
update heatprd.messages set amessage = 'Ended Heatwave.';
commit;
prompt Cleaning too short waves, allowing 1 normal day in between.
select end_day - start_day count_days, start_day, end_day, count(*) total
  from grid_025dd_temperature_waves 
 where end_day = to_date(&year || '&mmdd','YYYYMMDD') - 2
   and (end_day - start_day) < 2
group by end_day - start_day, start_day, end_day order by 2;
prompt Saving the data in case of error...
insert into grid_025dd_short_waves
select g2d_id, start_day, end_day, total_days, wave_type, temperature_extreme
 from  grid_025dd_temperature_waves
where  end_day = to_date(&year || '&mmdd','YYYYMMDD') - 2 
  and (end_day - start_day) < 2
;
delete from grid_025dd_temperature_waves 
      where end_day = to_date(&year || '&mmdd','YYYYMMDD') - 2
	    and (end_day - start_day) < 2
;
COMMIT;