REM ++++++++++++++++++++++++++++++++++++++++++++++
REM + Program  : sync_tempmin.sql                +
REM + Author   : Alfred de Jager                 +
REM + Creation : 8 September 2016                +
REM + Purpose  : Synchronize temperature data    +
REM +                                            +
REM ++++++++++++++++++++++++++++++++++++++++++++++
prompt Start Minimum Temperature Synchronization at
column tempcol    format A13 new_value atempcol
column daysback format 999 new_value adaysback
SELECT nvl('&1','2') daysback from dual;
SELECT to_char(sysdate,'Day DD Month YYYY HH24:MI') start_time 
      ,'temp_min_' || to_char(sysdate - &adaysback,'MMDD') tempcol
  FROM dual
;
declare
CURSOR getdata IS
SELECT g2d_id
     , year
     , &atempcol    avalue
     , min_stations_200km
     , max_stations_200km
  FROM grid_025dd_cold
 WHERE year = to_number(to_char(sysdate - &adaysback,'YYYY'))
ORDER BY g2d_id;
mydata getdata%rowtype;

CURSOR checkdata IS
 select rowid
   from grid_025dd_cold@toespositorain
 where g2d_id = mydata.g2d_id
   and   year = mydata.year
;        
mycheck  checkdata%rowtype;

myinsert number;
myupdate number;
myyear   number(4);
mystring varchar2(2000);

BEGIN
  OPEN getdata;
  myinsert := 0;
  myupdate := 0;
  LOOP
     FETCH getdata INTO mydata;
     EXIT WHEN getdata%NOTFOUND;
     myyear := mydata.year;
     OPEN checkdata;
     FETCH checkdata into mycheck;
     IF (checkdata%FOUND) then
          UPDATE grid_025dd_cold@toespositorain SET 
              &atempcol    = round(mydata.avalue)
             ,min_stations_200km = mydata.min_stations_200km
             ,max_stations_200km = mydata.max_stations_200km
           WHERE rowid = mycheck.rowid;
          myupdate := myupdate + 1;
     ELSIF (mydata.avalue IS NOT NULL) THEN
          INSERT INTO grid_025dd_cold@toespositorain (g2d_id,year, &atempcol, min_stations_200km, max_stations_200km) values
            (mydata.g2d_id,mydata.year
            ,round(mydata.avalue), mydata.min_stations_200km, mydata.max_stations_200km);
          myinsert := myinsert + 1;
     END IF;
     COMMIT;
     CLOSE checkdata;
  END LOOP;
  CLOSE getdata;
  mystring := 'comment on column grid_025dd_cold.&atempcol IS ' || '''' || 'Synchronized and Inserted ' || to_char(myinsert) || ' Updated ' || to_char(myupdate) || ' records for year ' || to_char(myyear) || '''';
  execute immediate mystring;
END;
/
.
prompt Finished temperature synchronization for &atempcol on
select to_char(sysdate,'Day DD Month YYYY HH24:MI') end_time from dual;