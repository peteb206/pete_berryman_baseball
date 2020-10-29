/* Location of the necessary json files */
%let path = C:\Users\peberr\OneDrive - SAS\BeatTheStreak;
libname bts "&path.";


/********************************************************************************************/
/********************************************************************************************/
/********************************   GAME LOGS   *********************************************/
/********************************************************************************************/
/******************************************************************************************/

/* Response of Fangraphs API */
filename GLogs temp;

/* GLogs.json parser*/
filename map "&path.\map.json";

/* POST Request to Fangraphs */
proc http
	out = GLogs
	url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
	in = '{"strPlayerId":"all","strSplitArr":[],"strGroup":"game","strPosition":"B","strType":1,"strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"true","arrPlayerId":[],"strSplitArrPitch":[]}';
	headers
		"Accept"="application/json, text/plain, */*"
		"Content-Type"="application/json;charset=UTF-8";
run;


libname Fgraphs json fileref=GLogs map=map automap=reuse;


data bts.glogs_2019 (drop = ordinal_root ordinal_v Date_Char);
	set Fgraphs.v;
	Date = input(Date_Char, yymmdd10.);
	format Date yymmdd10.;
run;

filename GLogs clear;
filename map clear;
libname Fgraphs clear;



/********************************************************************************************/
/********************************************************************************************/
/*****************************   PROBABLE STARTER STATS  ************************************/
/********************************************************************************************/
/********************************************************************************************/
/* Response of Fangraphs API */
filename spGLogs temp;

/* GLogs.json parser*/
filename map3 "&path.\map3.json";

/* POST Request to Fangraphs */
proc http
	out = spGLogs
	url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
	in = '{"strPlayerId":"all","strSplitArr":[42],"strGroup":"game","strPosition":"P","strType":"1","strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[{"stat":"IP","comp":"gt","low":"0","high":-99,"pending":false}],"strStatType":"player","strAutoPt":"false","arrPlayerId":[],"strSplitArrPitch":[]}';
	headers
		"Accept"="application/json, text/plain, */*"
		"Content-Type"="application/json;charset=UTF-8";
run;


libname Fgraphs json fileref=spGLogs map=map3 automap=reuse;


data bts.sp_glogs_2019 (drop = ordinal_root ordinal_v Date_Char);
	set Fgraphs.v;
	format Date yymmdd10. IP 4.1;
	Date = input(Date_Char, yymmdd10.);
	if mod(IP, 1) =  0.2 then IP = IP - 0.2 + 2 / 3;
	if mod(IP, 1) = 0.1 then IP = IP - 0.1 + 1 / 3;
run;

/* Today's Probable Starters */
/* https://blogs.sas.com/content/sasdummy/2017/12/04/scrape-web-page-data/ */
filename starters temp;
%let today = %sysfunc(today(), yymmdd10.);
proc http
 method="GET"
 url="https://www.fangraphs.com/livescoreboard.aspx?date=&today."
 out=starters;
run;

data bts.SP_Today (keep= playerID Name);
	length Starter $ 40;
	infile starters length=len lrecl=32767;
	input line $varying32767. len;
	line = strip(line);
 	if find(line, "SP: ");
 	line = transtrn(line, 'SP:', '*');
 	do i = 2 to (count(line, '*')+1);
 		playerID = scan(scan(line, i, '*'), 3, '=&');
 		Name = scan(scan(line, i, '*'), 3, '<>');
 		output;
 	end;
run;

filename spGLogs clear;
filename map3 clear;
libname Fgraphs clear;
filename starters clear;




/********************************************************************************************/
/********************************************************************************************/
/********************************   SUMMARY STATS  ******************************************/
/********************************************************************************************/
/********************************************************************************************/
/* Versus Left-Handed Pitchers */
/* Response of Fangraphs API */
filename vsLHP temp;

/* GLogs.json parser*/
filename splitMap "&path.\splitMap.json";

/* POST Request to Fangraphs */
proc http
	out = vsLHP
	url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
	in = '{"strPlayerId":"all","strSplitArr":[1],"strGroup":"season","strPosition":"B","strType":"1","strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"false","arrPlayerId":[],"strSplitArrPitch":[]}';
	headers
		"Accept"="application/json, text/plain, */*"
		"Content-Type"="application/json;charset=UTF-8";
run;


libname Fgraphs json fileref=vsLHP map=splitMap automap=reuse;


data bts.vsLHP (keep = playerId Name AVG);
	set Fgraphs.v;
run;

filename vsLHP clear;
filename splitMap clear;
libname Fgraphs clear;

/* Versus Right-Handed Pitchers */
/* Response of Fangraphs API */
filename vsRHP temp;

/* GLogs.json parser*/
filename splitMap "&path.\splitMap.json";

/* POST Request to Fangraphs */
proc http
	out = vsRHP
	url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
	in = '{"strPlayerId":"all","strSplitArr":[2],"strGroup":"season","strPosition":"B","strType":"1","strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"false","arrPlayerId":[],"strSplitArrPitch":[]}';
	headers
		"Accept"="application/json, text/plain, */*"
		"Content-Type"="application/json;charset=UTF-8";
run;


libname Fgraphs json fileref=vsRHP map=splitMap automap=reuse;


data bts.vsRHP (keep = playerId Name AVG);
	set Fgraphs.v;
run;

filename vsRHP clear;
filename splitMap clear;
libname Fgraphs clear;

/* At Home */
/* Response of Fangraphs API */
filename AtHome temp;

/* GLogs.json parser*/
filename map "&path.\map.json";

/* POST Request to Fangraphs */
proc http
	out = AtHome
	url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
	in = '{"strPlayerId":"all","strSplitArr":[7],"strGroup":"game","strPosition":"B","strType":1,"strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"true","arrPlayerId":[],"strSplitArrPitch":[]}';
	headers
		"Accept"="application/json, text/plain, */*"
		"Content-Type"="application/json;charset=UTF-8";
run;


libname Fgraphs json fileref=AtHome map=map automap=reuse;


data bts.atHome_2019 (drop = ordinal_root ordinal_v Date_Char);
	set Fgraphs.v;
	Date = input(Date_Char, yymmdd10.);
	format Date yymmdd10.;
run;

filename AtHome clear;
filename map clear;
libname Fgraphs clear;

/* On the Road */
/* Response of Fangraphs API */
filename OnRoad temp;

/* GLogs.json parser*/
filename map "&path.\map.json";

/* POST Request to Fangraphs */
proc http
	out = OnRoad
	url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
	in = '{"strPlayerId":"all","strSplitArr":[8],"strGroup":"game","strPosition":"B","strType":1,"strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"player","strAutoPt":"true","arrPlayerId":[],"strSplitArrPitch":[]}';
	headers
		"Accept"="application/json, text/plain, */*"
		"Content-Type"="application/json;charset=UTF-8";
run;


libname Fgraphs json fileref=OnRoad map=map automap=reuse;


data bts.onRoad_2019 (drop = ordinal_root ordinal_v Date_Char);
	set Fgraphs.v;
	Date = input(Date_Char, yymmdd10.);
	format Date yymmdd10.;
run;

filename OnRoad clear;
filename map clear;
libname Fgraphs clear;



/********************************************************************************************/
/********************************************************************************************/
/********************************   BULLPEN STATS  ******************************************/
/********************************************************************************************/
/********************************************************************************************/

/* Response of Fangraphs API */
filename Bpen temp;

/* GLogs.json parser*/
filename map2 "&path.\map2.json";

/* POST Request to Fangraphs */
proc http
	out = Bpen
	url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
	in = '{"strPlayerId":"all","strSplitArr":[43],"strGroup":"season","strPosition":"P","strType":1,"strStartDate":"2019-03-01","strEndDate":"2019-11-01","strSplitTeams":false,"dctFilters":[],"strStatType":"team","strAutoPt":"false","arrPlayerId":[],"strSplitArrPitch":[]}';
	headers
		"Accept"="application/json, text/plain, */*"
		"Content-Type"="application/json;charset=UTF-8";
run;


libname Fgraphs json fileref=Bpen map=map2 automap=reuse;


data bts.Bpen_2019 (drop = ordinal_root ordinal_v);
	set Fgraphs.v;
	format IP 5.1;
	if mod(IP, 1) =  0.2 then IP = IP - 0.2 + 2 / 3;
	if mod(IP, 1) = 0.1 then IP = IP - 0.1 + 1 / 3;
run;

filename Bpen clear;
filename map2 clear;
libname Fgraphs clear;



/********************************************************************************************/
/********************************************************************************************/
/********************************   TODAY'S GAMES  ******************************************/
/********************************************************************************************/
/********************************************************************************************/
%let today = %sysfunc(today(),yymmdd10.);
filename Games temp;
filename map4 "&path.\map4.json";

proc http
	out = Games
	url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=&today.";
run;


libname Fgraphs json fileref=Games map=map4 automap=reuse;
/*
proc format;
	value	$shortname 	'New York Yankees'		=	'NYY'
						'Boston Red Sox'		=	'BOS'
						'Toronto Blue Jays'		=	'TOR'
						'Tampa Bay Rays'		=	'TBR'
						'Baltimore Orioles'		=	'BAL'
						'Cleveland Indians' 	=	'CLE'
						'Detroit Tigers'		=	'DET'
						'Chicago White Sox'		=	'CHW'
						'Kansas City Royals'	=	'KCR'
						'Minnesota Twins'		=	'MIN'
						'Houston Astros'		=	'HOU'
						'Oakland Athletics'		=	'OAK'
						'Los Angeles Angels'	=	'LAA'
						'Texas Rangers'			=	'TEX'
						'Seattle Mariners'		=	'SEA'
						'Atlanta Braves'		=	'ATL'
						'Washington Nationals'	=	'WAS'
						'Philadelphia Phillies'	=	'PHI'
						'New York Mets'			=	'NYM'
						'Miami Marlins'			=	'MIA'
						'St. Louis Cardinals'	=	'STL'
						'Milwaukee Brewers'		=	'MIL'
						'Chicago Cubs'			=	'CHC'
						'Pittsburgh Pirates'	=	'PIT'
						'Cincinnati Reds'		=	'CIN'
						'Los Angeles Dodgers'	=	'LAD'
						'San Francisco Giants'	=	'SFG'
						'Arizona Diamondbacks'	=	'ARI'
						'Colorado Rockies'		=	'COL'
						'San Diego Padres'		=	'SDP'
;
run;
*/
proc sql;
create table bts.AwayTeams as
	select x.Abbreviation as Team, a.ordinal_team
	from bts.team_names as x, fgraphs.away_team as a
	where x.Team = a.name;

create table bts.HomeTeams as 
	select x.Abbreviation as Team, h.ordinal_team
	from bts.team_names as x, fgraphs.home_team as h
	where x.Team = h.name;

create table bts.matchups as
	select 'Away' as HomeAway,
		   a.Team as Team,
		   h.Team as Opponent 
	from bts.AwayTeams as a inner join bts.HomeTeams as h on a.ordinal_team = h.ordinal_team
	outer union corr 
	select 'Home' as HomeAway,
		   h.Team as Team,
		   a.Team as Opponent
	from bts.AwayTeams as a inner join bts.HomeTeams as h on a.ordinal_team = h.ordinal_team;
quit;

filename Games clear;
filename map4 clear;
libname Fgraphs clear;

proc sort data = bts.glogs_2019 out = current_team_b(keep=Name playerID Team date);
	by playerId descending date;
run;

proc sort data = bts.sp_glogs_2019 out = current_team_p(keep=Name playerID Team date);
	by playerId descending date ;
run;

data bts.current_team;
	set current_team_p current_team_b;
	by playerId;
	if first.playerId;
run;