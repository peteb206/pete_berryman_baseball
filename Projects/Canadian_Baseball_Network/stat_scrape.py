import requests
import pandas as pd

def scrape_stats(url, division, header):
    table_index = 0
    url_start = ''
    if division.startswith('NCAA'):
        url_start = 'http://stats.ncaa.org/player/game_by_game?game_sport_year_ctl_id=15580&stats_player_seq='
        table_index = 4
    elif division.startswith('JUCO'):
        url_start = 'https://www.njcaa.org/sports/bsb/2020-21/div1/players/'
        table_index = 2
    elif division == 'NAIA':
        url_start = 'http://www.dakstats.com/WebSync/Pages/Team/IndividualStats.aspx?association=10&sg=MBA&sea=NAIMBA_2021&'
        table_index = 2
    elif division == 'California CC':
        url_start = 'https://www.cccaasports.org/sports/bsb/2020-21/players/'
        table_index = 2
    elif division == 'NW Athletic Conference':
        url_start = 'https://nwacstats.org/sports/bsb/2019-20/players/'
        table_index = 2
    df = pd.read_html(requests.get(url_start + url, headers=header, timeout=10).text)[table_index]
    if division.startswith('NCAA'):
        new_header = df.iloc[1] # grab the first row for the header
        df = df[2:] # take the data less the header row
        df.columns = new_header # set the header row as the df header
    return df