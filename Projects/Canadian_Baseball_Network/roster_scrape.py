import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import numpy as np
import time


def get_team_websites():
    # set url and parameters
    baseUrl = 'https://www.collegebaseballhub.com/_api/wix-code-public-dispatcher/siteview/wix/data-web.jsw/find.ajax'
    gridAppId = '4544fd6a-5fe8-4482-a5fc-1fafab59ad5a'
    instance = 'wixcode-pub.b1c45a24c1814ed732fc488a1b3ad90c65889da3.eyJpbnN0YW5jZUlkIjoiZjFiMGI3NjUtMzM1OC00Mjc1LThkODItY2NmZWJiNTAyMGIwIiwiaHRtbFNpdGVJZCI6IjFkNmQxYzg5LTRlNTAtNGNiMy1hM2M3LTJkNzFmYjg0MDU5NCIsInVpZCI6bnVsbCwicGVybWlzc2lvbnMiOm51bGwsImlzVGVtcGxhdGUiOmZhbHNlLCJzaWduRGF0ZSI6MTYxMzY1OTY1MzE2NiwiYWlkIjoiMjVhOTNlMTAtM2NiYS00Y2IzLWExYTItZmRiMjEwY2U2OWE4IiwiYXBwRGVmSWQiOiJDbG91ZFNpdGVFeHRlbnNpb24iLCJpc0FkbWluIjpmYWxzZSwibWV0YVNpdGVJZCI6IjA3NzA4OGEwLWU2ODAtNDg4Ni1hMWY4LThmMjYxMGZjMDQwZiIsImNhY2hlIjpudWxsLCJleHBpcmF0aW9uRGF0ZSI6bnVsbCwicHJlbWl1bUFzc2V0cyI6IlNob3dXaXhXaGlsZUxvYWRpbmcsQWRzRnJlZSxIYXNEb21haW4sSGFzRUNvbW1lcmNlIiwidGVuYW50IjpudWxsLCJzaXRlT3duZXJJZCI6IjczODNhZGJlLTE3OGUtNDhhNS1hYTFiLTYyN2JmMTA1MWJmYiIsImluc3RhbmNlVHlwZSI6InB1YiIsInNpdGVNZW1iZXJJZCI6bnVsbH0='
    viewMode = 'site'
    params = {'gridAppId': gridAppId, 'instance': instance, 'viewMode': viewMode}

    # initalize dataframe
    schools_df = pd.DataFrame(columns=['title', 'division', 'conference', 'state', 'location', 'link'])
    schools_df

    for division in ['D1', 'D2', 'D3', 'NAIA']:
        # set request body
        request_body = ["Division1",{"$and":[{"$and":[]},{"$and":[]},{"$and":[]},{"orderId":{"$gt":0}},{"division":{"$eq":"{}".format(division)}}]},[{"orderId":"asc"}],0,500]

        # send post request
        r = requests.post(url = baseUrl, params = params, json = request_body) 

        # extracting response text
        json_string = r.text

        items = json.loads(json_string)['result']['items']
        df = pd.DataFrame(items, columns=['title', 'division', 'conference', 'state', 'location', 'link'])

        schools_df = schools_df.append(df, ignore_index=True)
    schools_df.sort_values(by=['division', 'title'], ignore_index=True, inplace=True)
    return schools_df


def get_roster_pages(schools_df, csv_export = False):
    # Case 1: http://www.______.com/index.aspx?path=...
    schools_df['roster_link'] = np.where(schools_df['link'].str.contains('\/(?:index|roster|schedule)\.aspx\?path=(?:baseball|base|bball|bb|bs|bsb|mbase)', regex=True),
                                     schools_df['link'].str.replace('\/(?:index|roster|schedule)\.aspx\?path=(?:baseball|base|bball|bb|bs|bsb|mbase).*', '/sports/baseball/roster', regex=True),
                                     '')

    # Case 2: https://www.______.com/sports/bsb/index
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('(?:\/landing|\/sports\/bsb)\/index$', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('(?:\/landing|\/sports\/bsb)\/index', '/sports/bsb/2020-21/roster', regex=True),
                                         schools_df['roster_link'])

    # Case 3: https://______.com/sports/baseball
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('\/baseball\/*$', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('\/baseball\/*', '/baseball/roster', regex=True),
                                         schools_df['roster_link'])

    # Case 4: https://______.com/sports/m-basebl
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('\/sports*\/m-basebl\/*.*$', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('\/m-basebl\/*.*', '/m-basebl/roster', regex=True),
                                         schools_df['roster_link'])

    # Case 5: https://______.com/sports/m-basebl/index
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('\/sports\/m-basebl\/index$', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('\/index', '/2020-21/roster', regex=True),
                                         schools_df['roster_link'])

    # Case 6: https://______.com/SportSelect.dbml...
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('SportSelect\.dbml.*', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('\/SportSelect\.dbml.*', '/sports/baseball/roster', regex=True),
                                         schools_df['roster_link'])

    # Case 7: https://______.com/sport/0/3
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('\/sport\/0\/3$', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('\/sport\/0\/3', '/roster/0/3', regex=True),
                                         schools_df['roster_link'])

    # Case 8: https://______.com/athletics/bb/
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('\/athletics\/bb\/*$', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('\/athletics\/bb\/*', '/athletics/bb/roster', regex=True),
                                         schools_df['roster_link'])

    # Case 9: http://______.com/sport.asp?sportID=1
    schools_df['roster_link'] = np.where((schools_df['link'].str.contains('\/sport.asp', regex=True)) & (schools_df['roster_link'] == ''),
                                         schools_df['link'].str.replace('\/sport.asp', '/roster.asp', regex=True),
                                         schools_df['roster_link'])

    schools_df['roster_link_flg'] = schools_df['roster_link'] != ''
    value_counts = schools_df['roster_link_flg'].value_counts()
    print('{} schools have a known roster URL, and {} schools have yet to be determined.'.format(value_counts[True], value_counts[False]))

    # Case 10: Manual Edits --- try to make this more computationally efficient eventually
    schools_df['roster_link'] = np.where(schools_df['title'] == 'Liberty University', 'https://www.liberty.edu/flames/index.cfm?PID=36959&teamID=1', schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'Keystone College', 'https://www.gokcgiants.com/sports/baseball/roster', schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'University of Dubuque', 'https://udspartans.com/sports/baseball/roster', schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'University of St. Thomas, Texas', 'https://www.ustcelts.com/sports/bsb/2020-21/roster',schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'Utica College', 'https://ucpioneers.com/sports/baseball/roster', schools_df['roster_link'])

    # missing_roster_link_df = schools_df[schools_df['roster_link'] == '']
    # display(missing_roster_link_df.style.set_properties(subset=['link'], **{'width-min': '500px'}))

    # Export to CSV
    if csv_export == True:
        schools_df[schools_df['roster_link'] != ''].drop(columns=['roster_link_flg']).to_csv('roster_pages.csv', index=False)

    return schools_df


def read_roster_norm(html):
    df = html[0]
    for temp_df in html:
        if len(temp_df.index) > len(df.index):
            df = temp_df
    return df


def read_roster(school, header):
    df = pd.DataFrame()
    response_text = requests.get(school['roster_link'], headers=header).text
    html = pd.read_html(response_text)
    response_has_table = len(html) > 0
    if response_has_table == True:
        df = read_roster_norm(html)
    df['__school'] = school['title']
    df['__division'] = school['division']
    return df


def filter_canadians(df):
    hometown_column = ''
    for column in df.columns:
        if column.lower().startswith('hom'):
            hometown_column = column
            break
    if hometown_column != '':
        df['__hometown_column'] = hometown_column
        searchfor = ['canada', 'ontario', 'quebec', 'nova scotia', 'new brunswick', 'manitoba', 'british columbia', 'prince edward island', 'saskatchewan', 'alberta', 'newfoundland']
        return df[df[hometown_column].str.contains('|'.join(searchfor), case=False, na=False)]
    return pd.DataFrame()


def iterate_over_schools(schools_df, outer=True, debug=True):
    # Start timer
    start_time = time.time()

    # Print helpful info
    index_col_length, title_col_length, players_col_length, canadians_col_length, roster_link_col_length = 6, 52, 9, 11, 80
    print('\nReading the rosters of {} schools...\nThis will take approximately {} minutes...\n'.format(str(len(schools_df.index)), str(int(round(len(schools_df.index) / 50, 0)))))
    border_row = '|{}|{}|{}|{}|{}|'.format('-'*index_col_length, '-'*title_col_length, '-'*players_col_length, '-'*canadians_col_length, '-'*roster_link_col_length)
    header_row = '|{}|{}|{}|{}|{}|'.format('#'.center(index_col_length), 'school'.center(title_col_length), 'players'.center(players_col_length), 'canadians'.center(canadians_col_length), 'roster_link'.center(roster_link_col_length))
    print(border_row)
    print(header_row)
    print(border_row)

    success_count = 0
    empty_roster_count = 0
    fail_count = 0
    fail_index_list = list()

    # Set header for requests
    header = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
      "X-Requested-With": "XMLHttpRequest"
    }

    # Initialize roster_df_list and canadians_df_list
    roster_df_list = list()
    canadians_df_list = list()

    # Iterate
    for index, school in schools_df.iterrows():
        print_row = '| {} | {} | {} | {} | {} |'.format(str(index).ljust(index_col_length-2), school['title'].ljust(title_col_length-2), '-'*(players_col_length-2), '-'*(canadians_col_length-2), school['roster_link'].ljust(roster_link_col_length-2))
        error_message = ''
        try:
            df = read_roster(school, header)
            canadians_df = filter_canadians(df)
            # Add team's roster to list 
            roster_df_list.append(df)
            canadians_df_list.append(canadians_df)
            print_row = print_row.replace('-'*(canadians_col_length-2), str(len(canadians_df.index)).center(canadians_col_length-2))
            print_row = print_row.replace('-'*(players_col_length-2), str(len(df.index)).center(players_col_length-2))
            if len(df.index) > 0:
                success_count += 1
            else:
                # Roster exists but with no players
                empty_roster_count += 1
        except Exception as e:
            error_message = str(e)
        if error_message != '':
            if outer == False:
                print_row = '| {} | {} | {} | {} | {}'.format(str(index).ljust(index_col_length-2), school['title'].ljust(title_col_length-2), '-'*(players_col_length-2), '-'*(canadians_col_length-2), school['roster_link'] + ': {}'.format(error_message))
            fail_index_list.append(index)
            fail_count += 1
        if debug == True:
            print(print_row)
    print(border_row)

    # Print results
    print('\n{} successes... {} empty rosters... {} failures\n'.format(str(success_count), str(empty_roster_count), str(fail_count)))
    if outer == True:
        print('\nRetrying the {} failures...'.format(str(fail_count)))
        # print('fail_index_list: ' + ', '.join([str(i) for i in fail_index_list]))
        # See which failures are legit and which are flukes
        iterate_over_schools(schools_df[schools_df.index.isin(fail_index_list)], outer = False, debug = debug)
        print('\n--- Total time: {} minutes ---'.format(str(round((time.time() - start_time) / 60, 1))))
    return roster_df_list, canadians_df_list


def print_cols(roster_df_list):
    players_df = pd.concat(roster_df_list, ignore_index=True)
    all_columns = list(players_df.columns.values)
    all_columns.sort()
    for col in all_columns:
        print(col)