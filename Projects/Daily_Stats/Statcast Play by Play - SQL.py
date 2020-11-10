import pandas as pd
import datetime as dt
from datetime import timedelta
import sqlite3
connection = sqlite3.connect('statcast.db')

date = dt.date(2019, 3, 20)
while date <= dt.date(2019, 9, 29):
    print(date)
    yesterday = str(date)
    pitch_by_pitch = pd.read_csv('https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea=2019%7C&hfSit=&player_type=batter&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfPull=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=xba&player_event_sort=h_launch_speed&sort_order=desc&min_pas=0&type=details'.format(yesterday, yesterday)
                             , parse_dates = ["game_date"])
    
    pitcher_name = pd.read_csv('https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7C&hfC=&hfSea=2019%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfPull=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=xba&player_event_sort=h_launch_speed&sort_order=desc&min_pas=0&type=details'.format(yesterday, yesterday)
                          , index_col = "pitcher")
    pitcher_name = pitcher_name[["player_name"]].rename(columns = {"player_name": "pitcher_name"})
    pitcher_name.drop_duplicates(keep = "first", inplace = True)
    
    pitch_by_pitch = pitch_by_pitch.merge(pitcher_name, how = 'left', on = "pitcher")
    pitch_by_pitch.sort_values(["game_date", "game_pk", "inning", "at_bat_number", "pitch_number"], inplace = True)
    
    starting_pitchers = pitch_by_pitch[pitch_by_pitch["inning"] == 1].groupby(["game_pk", "inning_topbot"]).first()
    starting_pitchers.reset_index(inplace = True)
    starting_pitchers["starting_pitcher"] = True
    starting_pitchers = starting_pitchers[["pitcher", "starting_pitcher"]]
    
    pitch_by_pitch = pitch_by_pitch.merge(starting_pitchers, how = 'left', on = 'pitcher')
    pitch_by_pitch["starting_pitcher"].fillna(value = False, inplace = True)
    pitch_by_pitch.set_index("sv_id", inplace = True)
    
    pitch_by_pitch.to_sql("statcast_all", con = connection, if_exists = "append", index = True)
    
    date = date + timedelta(1)