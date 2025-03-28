REM ++++++++++++++++++++++++++++++++++++++++++++++++++
REM + Program  : hazardlocation.sql                  +
REM + Author   : Alfred de Jager                     +
REM + Creation : 10 July 2017                        +
REM + Version  : 1.0                                 +
REM + Purpose  : Filling of wave_hazards table       +
REM +            with heatwaves above 40 degrees     +
REM +            and the maximum per larger Seaoutlet+
REM ++++++++++++++++++++++++++++++++++++++++++++++++++ 
prompt Starting Temperature Hazard search in Cold and Heatwaves up to 3 days ago...
declare cursor getoutlet is
select rowid
      ,id
  from seaoutlets
 where area_km2 > 4200
order by hdm_id, sea_id, commencement
;
myoutlet getoutlet%rowtype;

cursor getheathazard is
select mdsys.sdo_geometry(2001,8307,g.cell.sdo_point,null,null) point
     , g2d_id
     , end_day
	 , temperature_extreme
	 , wave_type
	 , total_days
  from grid_025dd g
      ,grid_025dd_temperature_waves
      ,seaoutlets s 
 where s.rowid = myoutlet.rowid
   and mdsys.sdo_relate(g.cell,s.polygon,'querytype=window mask=anyinteract') = 'TRUE'
   and g.id = g2d_id
   and total_days > 2
   and end_day > sysdate - 5
   and wave_type = 'H'
   and temperature_extreme > 39.9
order by end_day, temperature_extreme desc
;
myheathazard getheathazard%rowtype;
cursor getcoldhazard is
select mdsys.sdo_geometry(2001,8307,g.cell.sdo_point,null,null) point
     , g2d_id
     , end_day
	 , temperature_extreme
	 , wave_type
	 , total_days
  from grid_025dd g
      ,grid_025dd_temperature_waves
      ,seaoutlets s 
 where s.rowid = myoutlet.rowid
   and mdsys.sdo_relate(g.cell,s.polygon,'querytype=window mask=anyinteract') = 'TRUE'
   and g.id = g2d_id
   and total_days > 2
   and end_day > sysdate - 3
   and wave_type = 'C'
   and temperature_extreme < -19.9
order by end_day, temperature_extreme desc
;
mycoldhazard getcoldhazard%rowtype;

BEGIN
 open getoutlet;
 loop
   fetch getoutlet into myoutlet;
   exit when getoutlet%NOTFOUND;
   open getheathazard;
   fetch getheathazard into myheathazard;
   if (getheathazard%FOUND) then
	  insert into wave_hazards (sot_id, g2d_id, point, end_date, temperature_extreme, wave_type, total_days) values
	         (myoutlet.id, myheathazard.g2d_id, myheathazard.point, myheathazard.end_day, myheathazard.temperature_extreme, 'H', myheathazard.total_days);
	  commit;
   end if;
   close getheathazard;
   open getcoldhazard;
   fetch getcoldhazard into mycoldhazard;
   if (getcoldhazard%FOUND) then
	  insert into wave_hazards (sot_id, g2d_id, point, end_date, temperature_extreme, wave_type, total_days) values
         (myoutlet.id, mycoldhazard.g2d_id, mycoldhazard.point, mycoldhazard.end_day, mycoldhazard.temperature_extreme, 'C', mycoldhazard.total_days);
	  commit;
   end if;
   close getcoldhazard;
 end loop;
 close getoutlet;
END;
/
.
column name     format a20
column end_date format a27
column days     format 99
select name
      ,w.point.sdo_point.x longitude
      ,w.point.sdo_point.y latitude
      ,to_char(end_date,'Day DD Month YYYY') end_date
	  ,total_days days
      ,temperature_extreme	  
  from wave_hazards w
     , seaoutlets 
 where id = sot_id
   and end_date > sysdate - 5
;
prompt Ended Hazard Location.
   