import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time


def main():
    # Start timer
    start_time = time.time()

    players_df = pd.read_csv('canadians.csv')

    session = requests.Session()
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    summary_dicts = list()

    for index, player in players_df.iterrows():
        if (player['stats_link'] != '') & (type(player['stats_link']) == type('')):
            for url in player['stats_link'].split(','):
                print('-------------------------')
                print(index, player['division'], player['position'], ' --- ', '{} minutes elapsed'.format(str(round((time.time() - start_time) / 60, 1))))
                stats_df = scrape_sites(session, url, player['division'], header)
                if len(stats_df.index) > 0:
                    summary_dicts.append(collect_stats(player, stats_df))
                else:
                    print('No stats found for {}'.format(player['name']))

    stats_df = convert_dict_list_to_df(summary_dicts)
    stats_df.to_csv('stats.csv', index=False)


def scrape_sites(session, url, division, header):
    response = ''
    attempts = 0
    while (response == '') & (attempts <= 3):
        attempts += 1
        try:
            response = session.get(url, headers=header, timeout=10).text
        except:
            pass

    table_index = 0
    if division.startswith('NCAA'):
        table_index = 2
    elif (division.startswith('JUCO')) | (division == 'California CC') | (division == 'NW Athletic Conference'):
        table_index = 1
    return find_table(division, response, table_index)


def find_table(division, html, table_index):
    try:
        if division.startswith('NCAA'):
            df = pd.read_html(html)[table_index]
            new_header = df.iloc[1] # grab the first row for the header
            df = df[2:] # take the data less the header row
            df.columns = new_header # set the header row as the df header
        elif (division.startswith('JUCO')) | (division == 'California CC') | (division == 'NW Athletic Conference'):
            df = pd.read_html(html)[table_index]
        elif division == ('NAIA'):
            soup = BeautifulSoup(html, 'html')
            table = soup.find('table', {'id': 'ctl00_websyncContentPlaceHolder_overallStatsGridView'})
            df = pd.read_html(str(table))[table_index]
    except:
        return pd.DataFrame()
    return df


def collect_stats(player, df):
    out_dict = dict()

    # Standardize tables
    if 'Year' in df.columns:
        out_dict = read_ncaa_table(df)
    elif 'Opponent' in df.columns:
        out_dict = read_naia_table(df)
    else:
        out_dict = read_table(df)

    out_dict['Name'] = player['name']
    out_dict['Position'] = player['position']
    out_dict['School'] = player['school']
    out_dict['Division'] = player['division']
    return out_dict


def read_table(df):
    out_dict = dict()
    df_to_dict = dict(zip(df['Statistics category'], df['Overall']))
    stats_map = {
        'Games Played (GP)': 'Games',
        'Runs Scored (R)': 'Runs',
        'Hits (H)': 'Hits',
        'Doubles (2B)': 'Doubles',
        'Triples (3B)': 'Triples',
        'Home Runs (HR)': 'Home Runs',
        'Runs Batted In (RBI)': 'Runs Batted In',
        'Batting Average (AVG)': 'Batting Average',
        'Stolen Bases (SB)': 'Stolen Bases',
        'On-Base Percentage (OBP)': 'On Base Percentage',
        'Slugging Percentage (SLG)': 'Slugging Percentage',
        'Appearances (G)': 'Appearances',
        'Innings Pitched (IP)': 'Innings Pitched',
        'Wins (W)': 'Wins',
        'Earned Run Average (ERA)': 'Earned Run Average',
        'Saves (SV)': 'Saves',
        'Strikeouts (K)': 'Strikeouts'
    }

    for stat_out, stat_in in stats_map.items():
        if stat_in in df_to_dict.keys():
            out_dict[stat_out] = df_to_dict[stat_in]

    return out_dict


def read_ncaa_table(df):
    out_dict = dict()
    df_to_dict = df[df['Year'] == '2020-21'].to_dict('records')
    stats_map = {
        'Games Played (GP)': 'GP',
        'Runs Scored (R)': 'R',
        'Hits (H)': 'H',
        'Doubles (2B)': '2B',
        'Triples (3B)': '3B',
        'Home Runs (HR)': 'HR',
        'Runs Batted In (RBI)': 'RBI',
        'Batting Average (AVG)': 'BA',
        'Stolen Bases (SB)': 'SB',
        'On-Base Percentage (OBP)': 'OBPct',
        'Slugging Percentage (SLG)': 'SlgPct',
        'Appearances (G)': 'App',
        'Innings Pitched (IP)': 'IP',
        'Wins (W)': 'W',
        'Earned Run Average (ERA)': 'ERA',
        'Saves (SV)': 'SV',
        'Strikeouts (K)': 'SO'
    }

    if len(df_to_dict) > 0:
        df_to_dict = df_to_dict[0]
        for stat_out, stat_in in stats_map.items():
            if stat_in in df_to_dict.keys():
                out_dict[stat_out] = df_to_dict[stat_in]

    return out_dict


def read_naia_table(df):
    out_dict = dict()
    df_to_dict = df[df['Opponent'] == 'TOTALS'].to_dict('records')
    stats_map = {
        'Games Played (GP)': 'G',
        'Runs Scored (R)': 'R',
        'Hits (H)': 'H',
        'Doubles (2B)': '2B',
        'Triples (3B)': '3B',
        'Home Runs (HR)': 'HR',
        'Runs Batted In (RBI)': 'RBI',
        'Batting Average (AVG)': 'Avg',
        'Stolen Bases (SB)': 'SB',
        'Slugging Percentage (SLG)': 'Slg%',
        'Appearances (G)': 'G',
        'Innings Pitched (IP)': 'IP',
        'Wins (W)': 'W',
        'Earned Run Average (ERA)': 'ERA',
        'Saves (SV)': 'SV',
        'Strikeouts (K)': 'K'
    }

    # TO DO: ensure logs are 2021

    if len(df_to_dict) > 0:
        df_to_dict = df_to_dict[0]
        for stat_out, stat_in in stats_map.items():
            if stat_in in df_to_dict.keys():
                out_dict[stat_out] = df_to_dict[stat_in]

    if 'Slg%' in df_to_dict.keys(): # manually calculate OBP
        hits, walks, hbp, ab, sf = int(df_to_dict['H']), int(df_to_dict['BB']), int(df_to_dict['HBP']), int(df_to_dict['AB']), int(df_to_dict['SF'])
        numerator, denominator = hits + walks + hbp, ab + walks + hbp + sf
        if denominator > 0:
            out_dict['On-Base Percentage (OBP)'] = str(round(numerator / denominator, 3))

    return out_dict


def convert_dict_list_to_df(dict_list):
    df = pd.DataFrame(dict_list)
    df.replace(r'^-$', 0, regex=True, inplace=True)
    df.fillna(0, inplace=True)
    for col in ['Games Played (GP)', 'Runs Scored (R)', 'Hits (H)', 'Doubles (2B)', 'Triples (3B)', 'Home Runs (HR)', 'Runs Batted In (RBI)', 'Stolen Bases (SB)', 'Appearances (G)', 'Wins (W)', 'Saves (SV)', 'Strikeouts (K)']:
        df[col] = df[col].astype(int)
    for col in ['Innings Pitched (IP)', 'Earned Run Average (ERA)', 'Batting Average (AVG)', 'On-Base Percentage (OBP)', 'Slugging Percentage (SLG)']:
        df[col] = df[col].astype(float)
    df['On-Base plus Slugging (OPS)'] = (df['On-Base Percentage (OBP)'] + df['Slugging Percentage (SLG)']).round(3)
    return df


def update_gsheet(df):
    division_list = ['NCAA: Division 1', 'NCAA: Division 2', 'NCAA: Division 3', 'NAIA', 'JUCO: Division 1', 'JUCO: Division 2', 'JUCO: Division 3', 'California CC', 'NW Athletic Conference', 'USCAA']
    stat_list = ['Games Played (GP)', 'Runs Scored (R)', 'Hits (H)', 'Doubles (2B)', 'Triples (3B)', 'Home Runs (HR)', 'Runs Batted In (RBI)', 'Batting Average (AVG)', 'Stolen Bases (SB)', 'On-Base Percentage (OBP)', 'Slugging Percentage (SLG)', 'On-Base plus Slugging (OPS)', 'Appearances (G)', 'Innings Pitched (IP)', 'Wins (W)', 'Earned Run Average (ERA)', 'Saves (SV)', 'Strikeouts (K)']
    for division in division_list:
        print(division)
        df_by_division = df[df['Division'] == division].copy()
        for stat in stat_list:
            df_filtered = df_by_division.copy()
            ascending_flg = False
            if stat in ['Batting Average (AVG)', 'On-Base Percentage (OBP)', 'Slugging Percentage (SLG)', 'On-Base plus Slugging (OPS)']:
                df_filtered = df_filtered[(df_filtered['Hits (H)'] / df_filtered['Batting Average (AVG)'] >= 30) & (df_filtered[stat] > 0)] # At least 30 At Bats
            elif stat == 'Earned Run Average (ERA)':
                df_filtered = df_filtered[df_filtered['Innings Pitched (IP)'] >= 20] # At least 20 Innings Pitched
                ascending_flg = True
            else:
                df_filtered = df_filtered[df_filtered[stat] > 0] # Eliminate 0's
            df_filtered = df_filtered[['Name', 'Position', 'School', stat]]
            df_filtered.sort_values(by=stat, ascending=ascending_flg, ignore_index=True, inplace=True)
            display_count = 10 if len(df_filtered.index >= 10) else len(df_filtered.index)
            if display_count > 0:
                display(df_filtered.head(display_count))
                # To Do: include extra players if tie takes beyond 10 players
                # TBD: thresholds like AVG > 0.250 or ERA < 5.00 (Don't want to flaunt bad stats)


# Run main function
if __name__ == "__main__":
    main()