REM +++++++++++++++++++++++++++++++++++++++++++++++
REM + Program  : inv_diststations_temp_025.sql    +
REM + Author   : Alfred de Jager                  +
REM + Creation : 27 June 2016                     +
REM + Version  : 1.0                              +
REM +++++++++++++++++++++++++++++++++++++++++++++++

PROMPT Enter Days days before system date you want to process
column past format 99999 new_value apast
PROMPT Enter number of years before system year you want to process weather stations (needed if last_measurement is in the past year)
 -- DM, 2018-01-05
column wst_year_past format 99999 new_value awst_year_past
set linesize 100
select nvl('&1',3) past
      ,to_char(sysdate,'Day DD Month YYYY HH24.mi:ss') start_time
      ,nvl('&2',0) wst_year_past   -- DM, 2018-01-05
from dual;

column thetime    format a31  new_value atime
column thecol     format a13  new_value acol
column themonth   format a10  new_value amonth
column themonthnr format a8   new_value amonthnr
column theyear    format a4   new_value ayear
column thecomment format a103 new_value acomment

SELECT 
 'temp_max_' || to_char(sysdate - &apast,'MMDD') thecol
 ,trim(to_char(sysdate - &apast,'Month'))        themonth
 ,to_char(sysdate - &apast,'YYYYMMDD')           themonthnr
 ,to_char(sysdate - &apast,'YYYY')               theyear
 ,replace(replace(replace(to_char(sysdate,'Day DD Month YYYY HH24:MI.ss'),'  ',' '),'  ',' '),'  ',' ') thetime 
 ,'Maximum temperature for ' || to_char(sysdate - &apast,'YYYY') || ' month day ' || to_char(sysdate - &apast,'MMDD') thecomment
FROM dual;

COMMENT on COLUMN grid_025dd_heat.&acol  IS 'Computing new results for month &amonth in &ayear started on &atime';

DECLARE
cursor c_getdate IS
SELECT to_number(to_char(last_day(sysdate - &apast),'DD')) last_day
      ,to_char(sysdate - &apast,'MM') monthnr
      ,to_char(sysdate - &apast,'YYYY') year
  FROM DUAL;
mydate c_getdate%rowtype;

cursor c_getgrid IS
SELECT rowid
      ,id 
      ,elevation
      ,updated_by stations_200km
  FROM grid_025dd 
 where corr_cell is not null and elevation > -10
   and mdsys.sdo_relate(cell,sdo_geometry(2003,8307,null,mdsys.sdo_elem_info_array(1,1003,3)
                                                        ,mdsys.sdo_ordinate_array(-28,27,51,71.5)),
                                     'querytype=window mask=inside') = 'TRUE'
ORDER BY id;
mygrid c_getgrid%rowtype;

cursor c_getall IS
SELECT w.wmo_no
      ,mdsys.sdo_geom.sdo_distance(w.point,g.cell,0.0001) distance
      ,w.avg_day_temp
      ,w.altitude
  FROM grid_025dd      g
     , weather_station w
 WHERE mdsys.sdo_within_distance(w.point,g.cell,'distance=200000') = 'TRUE'
   AND g.rowid = mygrid.rowid
   AND w.avg_day_temp is not null
   AND to_number(to_char(w.last_measurement,'YYYY')) >= mydate.year-&awst_year_past  -- DM, 2018-01-05
ORDER BY 2
;
myall c_getall%rowtype;

mydisttot      number;
mytempsum      number;
mystationcount number(3);
mytotal        number;

cursor c_gettemp IS
select temp_min
      ,temp_max
      ,(myall.altitude * 0.0065) + temp_max temp_zero
  from cgms.metdata
 where station_number = myall.wmo_no
   and day = to_date('&amonthnr','YYYYMMDD')
   and temp_max is not null
;
mytemp c_gettemp%rowtype;

cursor c_heat is
select rowid
      ,min_stations_200km
      ,max_stations_200km
  from grid_025dd_heat
 where g2d_id = mygrid.id 
   and year   = &ayear
;
myheat c_heat%rowtype;

BEGIN
OPEN c_getdate;
OPEN c_getgrid;
FETCH c_getdate INTO mydate;
LOOP
  FETCH c_getgrid INTO mygrid;
  EXIT WHEN c_getgrid%NOTFOUND;
  mytotal      := 0;
  OPEN c_getall;
  mytempsum    := 0;
  mydisttot    := 0;
  mystationcount := 0;
  LOOP
    FETCH c_getall INTO myall;
    EXIT WHEN c_getall%NOTFOUND;
    OPEN c_gettemp;
    FETCH c_gettemp into mytemp;
    if (c_gettemp%FOUND) then
      if (myall.distance > 0) then 
        mytempsum := mytempsum + (mytemp.temp_zero / myall.distance);
        mydisttot := mydisttot + (1 / myall.distance);
      else
        mytempsum := mytempsum + (mytemp.temp_zero / 2500);
        mydisttot := mydisttot + (1 / 2500);
      end if;
      mystationcount := mystationcount + 1;
    end if;
    CLOSE c_gettemp;
    EXIT WHEN (mystationcount = 20);
  END LOOP;
  CLOSE c_getall;
  if (mystationcount > 0) then
    OPEN c_heat;
    FETCH c_heat into myheat;
    if (c_heat%FOUND) then
       if (mystationcount < myheat.min_stations_200km) then
         UPDATE grid_025dd_heat 
            SET &acol = round(mytempsum / mydisttot ) - (mygrid.elevation * 0.0065)
              , min_stations_200km = mystationcount 
          WHERE rowid = myheat.rowid;
        elsif (mystationcount > myheat.max_stations_200km) then
         UPDATE grid_025dd_heat 
            SET &acol = round(mytempsum / mydisttot ) - (mygrid.elevation * 0.0065)
              , max_stations_200km = mystationcount 
          WHERE rowid = myheat.rowid;
        else
         UPDATE grid_025dd_heat 
            SET &acol = round(mytempsum / mydisttot ) - (mygrid.elevation * 0.0065)
          WHERE rowid = myheat.rowid;
        end if;
    else
       INSERT INTO grid_025dd_heat (g2d_id, year, &acol, min_stations_200km, max_stations_200km) values
         (mygrid.id, &ayear, round(mytempsum / mydisttot) - (mygrid.elevation * 0.0065), mystationcount, mystationcount)
        ;
    end if;
    CLOSE c_heat;
    COMMIT;
   else
      open c_heat;
      fetch c_heat into myheat;
      if (c_heat%FOUND) then
        UPDATE grid_025dd_heat set &acol = null where rowid = myheat.rowid;
        COMMIT;
      end if;
      close c_heat;
   end if;
END LOOP;
CLOSE c_getgrid;
CLOSE c_getdate;
END;
/
.

COMMENT on COLUMN grid_025dd_heat.&acol IS '&acomment';
column amonth format a2 new_value parsemonth
column aday   format a2 new_value parseday
select to_char(sysdate,'Day DD Month YYYY HH24.mi:ss')  end_time
      ,substr('&amonthnr',5,2) amonth
      ,substr('&amonthnr',7,2) aday
 from dual;

prompt Computing the Maximum Temperature Anomaly ...
start e:\dea\heatprd\grid_025dd_heat_anomaly &ayear &parsemonth &parseday

prompt Synchronizing the external database ...
select floor(sysdate - to_date('&amonthnr','YYYYMMDD')) past
  from dual; 
start e:\dea\heatprd\sync_temp &apast

prompt Creating Maximum temperature maps on the External Website ...
host powershell -ExecutionPolicy RemoteSigned -File C:\DEV\generate.temperature.maps.ps1 url temp_max &ayear &parsemonth &parseday 0
host powershell -ExecutionPolicy RemoteSigned -File C:\DEV\generate.temperature.maps.ps1 url temp_anom &ayear &parsemonth &parseday 0

prompt Computing the Minimum Temperatures...
start e:\dea\heatprd\inv_diststations_tempmin_025 &apast
start e:\dea\heatprd\sync_tempmin &apast

prompt Creating Minimum temperature maps on the External Website ...
host powershell -ExecutionPolicy RemoteSigned -File C:\DEV\generate.temperature.maps.ps1 url temp_min &ayear &parsemonth &parseday 0
host powershell -ExecutionPolicy RemoteSigned -File C:\DEV\generate.temperature.maps.ps1 url temp_diff &ayear &parsemonth &parseday 0

start e:\dea\heatprd\dailyheatwaveturbo &ayear &parsemonth&parseday
start e:\dea\heatprd\sync_heatwaves
start e:\dea\heatprd\hazardlocation
start e:\dea\heatprd\refreshlastdays
start e:\dea\heatprd\update_dbio_endday &ayear &parsemonth &parseday

host powershell -ExecutionPolicy RemoteSigned -File C:\DEV\updateEdoLayerDatesJs.ps1 102 5 &ayear &parsemonth &parseday
host powershell -ExecutionPolicy RemoteSigned -File C:\DEV\updateEdoLayerDatesJs.ps1 103 5 &ayear &parsemonth &parseday
host powershell -ExecutionPolicy RemoteSigned -File C:\DEV\updateEdoLayerDatesJs.ps1 104 5 &ayear &parsemonth &parseday
pause
exit
