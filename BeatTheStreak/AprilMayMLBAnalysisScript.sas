/* Players with a PA in 2019 */
filename currPlrs temp;
proc http
 out = currPlrs
 url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
 in = '{"strPlayerId":"all","strSplitArr":[],"strGroup":"season","strPosition":"B","strType":"2","strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"false","arrPlayerId":[],"strSplitArrPitch":[]}';
 headers
  "Accept"="application/json, text/plain, */*"
  "Content-Type"="application/json;charset=UTF-8";
run;
libname currPlrs json fileref=currPlrs;
/* End Players with a PA in 2019 */

/* April/May Hitter Data */
filename hMonthly temp;
proc http
 out = hMonthly
 url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
 in = '{"strPlayerId":"all","strSplitArr":[84,85],"strGroup":"career","strPosition":"B","strType":"2","strStartDate":"2000-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"true","arrPlayerId":[],"strSplitArrPitch":[]}';
 headers
  "Accept"="application/json, text/plain, */*"
  "Content-Type"="application/json;charset=UTF-8";
run;
libname hMonthly json fileref=hMonthly;
/* End April/May Hitter Data */

/* Career Hitter Data */
filename hCareer temp;
proc http
 out = hCareer
 url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
 in = '{"strPlayerId":"all","strSplitArr":[],"strGroup":"career","strPosition":"B","strType":"2","strStartDate":"2000-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"true","arrPlayerId":[],"strSplitArrPitch":[]}';
 headers
  "Accept"="application/json, text/plain, */*"
  "Content-Type"="application/json;charset=UTF-8";
run;
libname hCareer json fileref=hCareer;
/* End Career Hitter Data */

/* Prep data for Visual Analytics */
proc sql number;
create table hitter as
select *
     , 'April/May' as context
from hMonthly.data
where
   playerId in (
      select playerId 
      from currPlrs.data
   )

outer union corr

select *
     , 'Career' as context
from hCareer.data
where
   playerId in (
      select playerId 
      from currPlrs.data
   )
;

/* Analysis */
select 
   a.playerId               label='Player ID'
 , a.playerName             label='Name'
 , a.wRC_ as wRC_AprilMay   label='wRC+ April/May'  format=12.2
 , b.wRC_ as wRC_Career     label='wRC+ Career'     format=12.2
 , a.wRC_ - b.wRC_ as diff  label='wrC+ Difference' format=12.2
from 
   hMonthly.data as a 
      join 
   hCareer.data as b 
      on a.playerId = b.playerId
where
   a.playerId in (
      select playerId 
      from currPlrs.data
   )
order by 
   diff desc
;
quit;

filename currPlrs clear;
filename hMonthly clear;
filename hCareer clear;
libname currPlrs clear;
libname hMonthly clear;
libname hCareer clear;