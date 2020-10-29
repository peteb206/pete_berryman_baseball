%let path = C:\Users\peberr\OneDrive - SAS\BeatTheStreak;
libname bts "&path.";
%let recent = 10 /* last __ days */;


/********************************************************************************************/
/********************************************************************************************/
/*************************        2019 SEASON        ****************************************/
/********************************************************************************************/
/********************************************************************************************/
proc sql number;
create view summary_all as
	select distinct(Name),
		playerID,
		sum(H) as Hits,
		sum(AB) / sum(G) as ABperG format=4.2,
		sum(H) / sum(G) as HperG format=4.2,
		sum(H > 0) as games_with_hit 'HG',
		sum(G) as games 'G',
		std(H) as std format=4.2,
		calculated games_with_hit / calculated games as games_hit_pct 'PCT' format=percent6.2,
		sum(H) / sum(AB) as AVG format=5.3
	from bts.glogs_2019
	group by playerID;
quit;
	


/********************************************************************************************/
/********************************************************************************************/
/*************************        2019 Splits        ****************************************/
/********************************************************************************************/
/********************************************************************************************/
proc sql;
create table splits as
	select distinct all.Name,
	all.playerId,
	rhp.AVG as AVG_vs_RHP format=5.3,
	lhp.AVG as AVG_vs_LHP format=5.3,
	sum(home.H) / sum(home.AB) as AVG_home format=5.3,
	sum(home.H > 0) / sum(home.G) as PCT_home format=percent6.2,
	sum(home.H) / sum(home.G) as HperG_home format=4.2,
	sum(road.H) / sum(road.AB) as AVG_road format=5.3,
	sum(road.H > 0) / sum(road.G) as PCT_road format=percent6.2,
	sum(road.H) / sum(road.G) as HperG_road format=4.2
	from summary_all as all
		left join bts.vsRHP as rhp on all.playerId = rhp.playerId
		left join bts.vsLHP as lhp on all.playerId = lhp.playerId
		left join bts.athome_2019 as home on all.playerId = home.playerId
		left join bts.onroad_2019 as road on all.playerId = road.playerId
	group by all.playerId;
run;
	


/********************************************************************************************/
/********************************************************************************************/
/*************************        HOT HITTERS        ****************************************/
/********************************************************************************************/
/********************************************************************************************/
proc sql number;
create view summary_&recent. as
	select distinct Name,
		playerID,
		Team,
		sum(H) as Hits,
		sum(H > 0) as games_with_hit 'HG',
		sum(G) as games 'G',
		(calculated Hits) / (calculated games) as HitsPerGame 'H/G' format=4.2,
		std(H) as std format=4.2,
		calculated games_with_hit / calculated games as games_hit_pct 'PCT' format=percent6.2,
		sum(H) / sum(AB) as AVG format=5.3
	from bts.glogs_2019
	where Date >= (today()-&recent.)
	group by playerID
	having HitsPerGame >= 1
	order by games_hit_pct desc, Hits desc;
quit;

	


/********************************************************************************************/
/********************************************************************************************/
/**************************         STARTERS        *****************************************/
/********************************************************************************************/
/********************************************************************************************/
proc sql number;
create table starters_stats as
	select distinct(year.playerID),
		   year.Name,
		   team.Team,
		   count(*) as GS,
		   sum(IP) as IP format=5.1,
		   (calculated IP / calculated GS) as IPperStart 'IP/GS' format=5.2,
		   sum(H) as H,
		   sum(H) / sum(IP) as HIP format=5.2,
		   sum(ER) / sum(IP) * 9 as ERA format=6.2
	from bts.sp_glogs_2019 as year
		 right join bts.sp_today as today on year.playerID = today.playerID
		 left join bts.current_team as team on year.playerID = team.playerID
	group by year.playerID
	order by HIP desc;
quit;



/********************************************************************************************/
/********************************************************************************************/
/**************************         BULLPENS        *****************************************/
/********************************************************************************************/
/********************************************************************************************/
proc sql number;
create view Bpen_rank as
	select Team,
		   IP,
		   (H/IP) as HIP format=5.2
	from bts.bpen_2019
	order by HIP desc;
quit;



ods html body = "C:\Users\peberr\Desktop\BTS.html";
/********************************************************************************************/
/********************************************************************************************/
/**************************         FULL DATA        ****************************************/
/********************************************************************************************/
/********************************************************************************************/
%let today = %sysfunc(today(), weekdate35.);
%let time = %sysfunc(time(), timeampm11.);
title "Hitter Data: &today.";
title2 "Season and Last &recent. days";
title3 "with Opponent Pitching Stats";
footnote "Updated &today. at &time.";
proc sql number;
	select all.Name,
		   team.Team,
		   m.HomeAway 'Home/Away',
		   m.Opponent,
		   sp.Name as SP 'Opposing Starter',
		   sp.HIP as spHIP,
		   ((sp.HIP * sp.IPperStart) + ((9 - sp.IPperStart) * bp.HIP)) as xHA format=5.2,
		   all.ABperG 'xAB',
   		   case m.HomeAway when "Home" then splits.HperG_home
   				   		   when "Away" then splits.HperG_road end as HperG_home_away '.' format=1.,
   		   mean(all.HperG, two.HitsperGame, calculated HperG_home_away) as xH format=5.2,
		   all.Hits 'H',
		   all.games_hit_pct 'PCT',
		   two.games_hit_pct "PCT&recent.",
		   all.AVG 'AVG',
		   two.AVG "AVG&recent.",
		   case m.HomeAway when "Home" then splits.AVG_home
		   				   when "Away" then splits.AVG_road end as AVG_home_away format=5.3,
		   splits.AVG_vs_RHP,
		   splits.AVG_vs_LHP
	from summary_all as all 
		inner join summary_&recent. as two on all.playerID = two.playerID
		left join bts.current_team as team on all.playerID = team.playerID
		inner join bts.matchups as m on team.Team = matchups.Team
		left join starters_stats as sp on m.Opponent = sp.Team
		left join bpen_rank as bp on m.Opponent = bp.Team
		left join splits as spl on all.playerId = spl.playerId
	having all.games >= 10 and (all.games_hit_pct >= 0.7 or all.AVG > 0.3) and AVG_home_away > 0.250 and two.AVG > 0.250
	order by all.games_hit_pct desc, two.AVG desc;
quit;
title;
title2;
title3;
footnote;

title 'Starting Pitchers';
proc print data=starters_stats;
run;
title;

title 'Bullpens';
proc print data=bpen_rank;
run;
title;

title 'Team Summary';
	select team,
		   sum(H > 0) / sum(G) as PCT format=percent6.2
	from bts.glogs_2019
	group by team
	order by PCT desc;
title 'League Summary by Date';
proc sql;
	select date,
		   sum(H > 0) / sum(G) as PCT format=percent6.2
	from bts.glogs_2019
   where date >= (today()-&recent.)
	group by date;
title 'League Summary - 2019';
	select sum(H > 0) / sum(G) as PCT format=percent6.2
	from bts.glogs_2019;
run;
title;

ods html close;
libname bts clear;