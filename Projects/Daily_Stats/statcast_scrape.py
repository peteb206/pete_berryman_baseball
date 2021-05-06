import pandas as pd
import os
import datetime
import time
from IPython.display import HTML


def main():
    today = datetime.date.today()
    statcast_df = get_statcast_data(today)

    calculations_df = calculate_hit_pct(statcast_df)
    last_x_days = 10
    x_days_ago = today - datetime.timedelta(days=last_x_days + 1)
    calculations_df_x_days = calculate_hit_pct(statcast_df, since_date=x_days_ago.strftime('%Y-%m-%d'))
    calculations_df_x_days.drop(['player_name', 'team'], axis=1, inplace=True)

    all_df = pd.merge(calculations_df, calculations_df_x_days, how='left', on='player_id', suffixes=('_total', f'_{last_x_days}'))
    all_df = all_df[all_df['G_total'] >= 10]

    pd.set_option('expand_frame_repr', False)
    print('\n', 'First 30 players:', '\n', '\n', all_df.head(30), sep='')
    all_df.to_json('calculations_{}.json'.format(today.year), orient='records')


def get_statcast_data(today):
    # Start timer
    start_time = time.time()

    this_year = today.year
    csv_name = 'statcast_{}.csv'.format(this_year)

    start_date = datetime.date(this_year, 1, 1)
    six_days, seven_days = datetime.timedelta(days=6), datetime.timedelta(days=7)

    df_list = list()
    existing_data_df = pd.read_csv(csv_name) if os.path.isfile(csv_name) else pd.DataFrame()
    if len(existing_data_df.index) > 0:
        df_list.append(existing_data_df)
        last_date = existing_data_df['game_date'].values[-1]
        print(f'{csv_name} has data up to {last_date}', '\n', sep='')
        last_date_split = last_date.split('-')
        start_date = datetime.date(this_year, int(last_date_split[1]), int(last_date_split[2])) + datetime.timedelta(days=1)

    while start_date <= today:
        start_date_str, end_date_str = start_date.strftime('%Y-%m-%d'), (start_date + six_days).strftime('%Y-%m-%d')
        df = pd.read_csv(
            'https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2021%7C&hfSit=&player_type=batter&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0&type=details&'.format(start_date_str, end_date_str),
            usecols=['game_pk', 'game_date', 'away_team', 'home_team', 'inning', 'inning_topbot', 'at_bat_number', 'player_name', 'batter', 'pitcher', 'events', 'stand', 'p_throws', 'estimated_ba_using_speedangle', 'babip_value']
        )
        obs = len(df.index)
        interval = ' to '.join([start_date_str, end_date_str])
        print(interval, f'{obs} results', sep=': ')

        if obs > 0:
            # Format data
            df = df[(~df['events'].isna()) & (df['events'] != '')]
            df['hit'] = df['events'].apply(lambda x: 1 if x in ['home_run', 'triple', 'double', 'single'] else 0)
            df['home_away'] = df['inning_topbot'].apply(lambda x: 'home' if x == 'Bot' else 'away')
            df['team'] = df.apply(lambda row: row['home_team'] if row['home_away'] == 'home' else row['away_team'], axis=1)
            df.sort_values(by=['game_date', 'game_pk', 'at_bat_number'], ignore_index=True, inplace=True)
            df.rename({'estimated_ba_using_speedangle': 'xBA'}, axis=1, inplace=True)
            df = df[['game_date', 'game_pk', 'player_name', 'team', 'batter', 'pitcher', 'events', 'stand' ,'p_throws', 'home_away', 'hit', 'xBA']] # keep rows that ended at bat
            df_list.append(df)

        start_date += seven_days

    df = pd.concat(df_list, ignore_index=True)
    df.to_csv(csv_name, index=False)

    # Stop timer
    print('\n', 'Done retrieving statcast data!', '\n', '\n', '--- Total time: {} minutes ---'.format(str(round((time.time() - start_time) / 60, 2))), sep='')
    return df


def calculate_hit_pct(statcast_df, since_date=None):
    keep_cols = ['player_name', 'team']
    if since_date != None:
        statcast_df = statcast_df[statcast_df['game_date'] >= since_date]
    df_by_game = statcast_df.groupby(['game_pk', 'batter'] + keep_cols)[['hit', 'xBA']].sum().reset_index().rename({'hit': 'H', 'xBA': 'xH'}, axis=1)
    df_by_game['H_1+'], df_by_game['xH_1+'] = (df_by_game['H'] >= 1).astype(int),  (df_by_game['xH'] >= 1).astype(int)

    df_by_game['player_id'] = df_by_game['batter']
    keep_cols = ['player_id'] + keep_cols

    df_by_season = df_by_game.groupby(keep_cols).agg({'H': 'sum', 'batter': 'count', 'H_1+': 'sum', 'xH_1+': 'sum'}).reset_index().rename({'batter': 'G'}, axis=1)
    df_by_season['hit_pct'] = df_by_season['H_1+'] / df_by_season['G']
    df_by_season['x_hit_pct'] = df_by_season['xH_1+'] / df_by_season['G']
    return df_by_season.sort_values(by='x_hit_pct', ascending=False, ignore_index=True)


# Run main function
if __name__ == "__main__":
    main()