import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import numpy as np
import time
import logging
import datetime
import psutil
import csv


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logging.getLogger('numexpr').setLevel(logging.WARNING)
global logger
logger = logging.getLogger()
logger.addHandler(logging.FileHandler('scraper.log', 'w'))


def check_cpu_and_memory():
    logger.debug('CPU percent: {}% --- Memory % Used: {}%'.format(str(psutil.cpu_percent()), str(psutil.virtual_memory()[2])))


def main():

    # Get Team Websites
    # Only applies for NCAA Divsion 1, NCAA Divsion 2, NCAA Division 3 and NAIA
    # Re-run this if needed, but most roster links should now be in the CSV files
    # schools_df = get_team_websites()
    # schools_df

    # Get Roster Pages
    # Only applies for NCAA Divsion 1, NCAA Divsion 2, NCAA Division 3 and NAIA
    # Re-run this if needed, but most roster links should now be in the CSV files
    # schools_df = get_roster_pages(schools_df)
    # schools_df

    # Get roster sites
    schools_df = pd.read_csv('roster_pages.csv')
    # Ask for input
    schools_df = get_input(schools_df)

    # Last run:
    last_run = 'Last run: ' + str(datetime.datetime.now().strftime("%B %d, %Y at %I:%M %p"))
    logger.info('')
    logger.info(last_run)

    # Iterate over schools
    df_lists = iterate_over_schools(schools_df)
    # roster_df_list = df_lists[0]
    canadians_dict_list = df_lists[1]

    # Periodically check all of the table columns found in the html to see if we are overlooking anything
    # print_cols(roster_df_list)

    # Format dictionaries to dataframe
    # pd.DataFrame(canadians_dict_list).to_csv('canadians_raw.csv', index=False)
    canadians_df = format_df(canadians_dict_list, schools_df)
    canadians_df = pd.concat([canadians_df, pd.read_csv('canadians_manual.csv')], ignore_index=True) # Add players who could not be scraped
    canadians_df.to_csv('canadians.csv', index=False)
    generate_html(canadians_df[['name','position','class','school','division','state','hometown']], 'canadians.html', last_run)
    logger.info('')
    logger.info('{} Canadian players found...'.format(str(len(canadians_df.index))))


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
    logger.info('{} schools have a known roster URL, and {} schools have yet to be determined.'.format(value_counts[True], value_counts[False]))

    # Case 10: Manual Edits --- try to make this more computationally efficient eventually
    schools_df['roster_link'] = np.where(schools_df['title'] == 'Liberty University', 'https://www.liberty.edu/flames/index.cfm?PID=36959&teamID=1', schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'Keystone College', 'https://www.gokcgiants.com/sports/baseball/roster', schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'University of Dubuque', 'https://udspartans.com/sports/baseball/roster', schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'University of St. Thomas, Texas', 'https://www.ustcelts.com/sports/bsb/2020-21/roster',schools_df['roster_link'])
    schools_df['roster_link'] = np.where(schools_df['title'] == 'Utica College', 'https://ucpioneecom/sports/baseball/roster', schools_df['roster_link'])

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
    response = ''
    try:
        response = requests.get(school['roster_link'], headers=header, timeout=10) # Try once
    except Exception as e:
        time.sleep(1)
        response = requests.get(school['roster_link'], headers=header, timeout=10) # Try one more time
    response_text = response.text
    soup = BeautifulSoup(response_text, 'lxml')
    if len(soup('table')) > 0:
        html = pd.read_html(response_text)
        df = read_roster_norm(html)
        df['__school'] = school['title']
        df['__division'] = school['division']
        df['__state'] = school['state']
    else:
        return str(response_text)
    return df


def filter_canadians(df, canada_strings):
    out_list = list()
    roster_dict = df.to_dict(orient='records')
    index = 0
    while index < len(roster_dict):
        player = roster_dict[index]
        for attr in player:
            value = str(player[attr])
            if any(canada_string.lower() in value.lower() for canada_string in canada_strings):
                player['__hometown'] = value
                out_list.append(player)
                break
        index += 1
    return out_list


def get_input(schools_df):
    run_all_rows = ''
    while run_all_rows not in ['y', 'n']:
        run_all_rows = input("\nRun script for all schools? Answer y/n... ")
        if run_all_rows not in ['y', 'n']:
            logger.error('Value must be "y" or "n"')
    if run_all_rows == 'n':
        run_first_last_rows = ''
        while run_first_last_rows not in ['first', 'last']:
            run_first_last_rows = input("\nRun first X schools or last X schools? Answer first/last... ")
            if run_first_last_rows not in ['first', 'last']:
                logger.error('Value must be "first" or "last"')
        number_of_rows = 0
        max_length = len(schools_df.index)
        while (number_of_rows < 1) | (number_of_rows > max_length):
            number_of_rows = input("\nHow many schools? ")
            if number_of_rows.isdigit() == False:
                logger.error('Value must be a positive integer less than or equal to ' + str(max_length))
            else:
                number_of_rows = int(number_of_rows)
            if number_of_rows > max_length:
                logger.error('Value must be a positive integer less than or equal to ' + str(max_length))
        if run_first_last_rows == 'first':
            schools_df = schools_df.head(number_of_rows)
        elif run_first_last_rows == 'last':
            schools_df = schools_df.tail(number_of_rows)
    return schools_df


def iterate_over_schools(schools_df):
    # Start timer
    start_time = time.time()

    # Print helpful info
    index_col_length, title_col_length, players_col_length, canadians_col_length, roster_link_col_length = 6, 52, 9, 11, 80
    logger.info('')
    logger.info('Reading the rosters of {} schools... This will take approximately {} minutes...'.format(str(len(schools_df.index)), str(int(round(len(schools_df.index) / 50, 0)))))
    logger.info('')
    border_row = '|{}|{}|{}|{}|{}|'.format('-'*index_col_length, '-'*title_col_length, '-'*players_col_length, '-'*canadians_col_length, '-'*roster_link_col_length)
    header_row = '|{}|{}|{}|{}|{}|'.format('#'.center(index_col_length), 'school'.center(title_col_length), 'players'.center(players_col_length), 'canadians'.center(canadians_col_length), 'roster_link'.center(roster_link_col_length))
    logger.info(border_row)
    logger.info(header_row)
    logger.info(border_row)

    success_count = 0
    empty_roster_count = 0
    fail_count = 0
    fail_index_list = list()
    schools_to_check_manually = list()
    canada_strings = ['canada', ', ontario', 'quebec', 'nova scotia', 'new brunswick', 'manitoba', 'british columbia', 'prince edward island', 'saskatchewan', 'alberta', 'newfoundland', ', b.c.', ', on', ', alta.', ', mb', ', man.', ', sask.', 'q.c.', 'qc', ', bc']

    # Set header for requests
    header = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
      "X-Requested-With": "XMLHttpRequest"
    }

    # Initialize roster_df_list and canadians_df_list
    roster_df_list = list()
    canadians_dict_list = list()

    # Iterate
    for index, school in schools_df.iterrows():
        print_row = '| {} | {} | {} | {} | {} |'.format(str(index).ljust(index_col_length-2), school['title'].ljust(title_col_length-2), '-'*(players_col_length-2), '-'*(canadians_col_length-2), str(school['roster_link']).ljust(roster_link_col_length-2))
        error_message = ''
        try:
            df = read_roster(school, header)
            canadian_count = '0'
            if type(df) == type(pd.DataFrame()): # Table(s) exist in HTML
                # Add team's roster to list 
                roster_df_list.append(df)
                canadians_dicts = filter_canadians(df, canada_strings) # Get a list of table rows that seem to have a Canadian player
                canadian_count = str(len(canadians_dicts))
            elif type(df) == type(''): # No table(s) exist in HTML... search all text for certain strings
                if any(canada_string.lower() in df.lower() for canada_string in canada_strings):
                    canadian_count = '*****' # String of interest found in HTML text
                    schools_to_check_manually.append('{}: {}'.format(school['title'], school['roster_link']))
                df = pd.DataFrame()
            if (canadian_count != '0') & (canadian_count != '*****'):
                canadians_dict_list.extend(canadians_dicts)
            print_row = print_row.replace('-'*(canadians_col_length-2), canadian_count.center(canadians_col_length-2))
            print_row = print_row.replace('-'*(players_col_length-2), str(len(df.index)).center(players_col_length-2))
            if len(df.index) > 0:
                logger.debug(print_row)
                success_count += 1
            else:
                # Roster exists but with no players
                logger.info(print_row)
                empty_roster_count += 1
        except Exception as e:
            error_message = str(e)
        if error_message != '':
            print_row = '| {} | {} | {} | {} | {}'.format(str(index).ljust(index_col_length-2), school['title'].ljust(title_col_length-2), '-'*(players_col_length-2), '-'*(canadians_col_length-2), str(school['roster_link']) + ': {}'.format(error_message))
            fail_index_list.append(index)
            fail_count += 1
            logger.info(print_row)
        # check_cpu_and_memory()
        time.sleep(0.05)
    logger.info(border_row)

    # Print results
    logger.info('')
    logger.info('{} successes... {} empty rosters... {} failures...'.format(str(success_count), str(empty_roster_count), str(fail_count)))
    if len(schools_to_check_manually) > 0:
        logger.info('')
        logger.info('Check these schools manually - could not process roster but may contain Canadians...')
        for school_and_link in schools_to_check_manually:
            logger.info('--- {}'.format(school_and_link))
    logger.info('')
    logger.info('--- Total time: {} minutes ---'.format(str(round((time.time() - start_time) / 60, 1))))
    return roster_df_list, canadians_dict_list


def print_cols(roster_df_list):
    players_df = pd.concat(roster_df_list, ignore_index=True)
    all_columns = [str(i) for i in list(players_df.columns.values)]
    all_columns.sort()
    for col in all_columns:
        logger.info(col)


def format_player_name(string):
    return ' '.join(string.split(',')[::-1]).strip() # Format as "First Last"


def format_player_class(string):
    # Output Freshman, Sophomore, Junior or Senior
    grad_year_map = {"'21": "Senior", "'22": "Junior", "'23": "Sophomore", "'24": "Freshman"}
    if string in grad_year_map.keys():
        return grad_year_map[string]

    if ('j' in string.lower()) | ('3' in string):
        return 'Junior'
    elif ('so' in string.lower()) | (string.lower() == 's') | ('2' in string):
        return 'Sophomore'
    elif ('f' in string.lower()) | ('1' in string) | ('hs' in string.lower()):
        return 'Freshman'
    elif ('sr' in string.lower()) | ('gr' in string.lower()) | ('4' in string) | ('5' in string):
        return 'Senior'
    return string


def format_player_position(string):
    # Ouput position(s) in acronym form separated by a forward slash
    return ''


def format_player_division(string):
    level = 'Division ' + string[-1]
    if string.upper() == 'NAIA':
        return string.upper()
    elif 'JUCO' in string.upper():
        return 'Junior Colleges and Community Colleges: ' + level
    else:
        return 'NCAA: ' + level


def format_player_hometown(string):
    # Remove attached High School name if, necessary
    # To Do: remove references to anything other than city and province
    return string.split('/')[0].strip()


def format_df(dict_list, schools_df):
    new_dict_list = list()
    for dictionary in dict_list:
        new_dict = dict()

        cols = ['name', 'position', 'b', 't', 'class', 'school', 'division', 'state', 'hometown', 'obj']
        for col in cols:
            if col != 'state':
                new_dict['__' + col] = ''
        new_dict['_first_name'] = ''
        new_dict['_last_name'] = ''

        for key, value in dictionary.items():
            key_str = str(key).lower()
            value_str = str(value)
            value_str = value_str.split(':')[-1].strip()
            if (len(value_str) != 0) & (value_str.lower() != 'nan'):
                # Set __class column
                if (new_dict['__class'] == '') & (key_str.startswith('cl') | key_str.startswith('y') | key_str.startswith('e') | key_str.startswith('ci.') | ('year' in key_str)):
                    new_dict['__class'] = format_player_class(value_str)

                # Set __name columns
                elif ('first' in key_str) & ('last' not in key_str):
                    new_dict['_first_name'] = value_str
                elif (key_str == 'last') | (('last' in key_str) & ('nam' in key_str)):
                    new_dict['_last_name'] = value_str
                elif ('name' in key_str) | (key_str == 'player'):
                    new_dict['__name'] = format_player_name(value_str) # Format as "First Last"

                # Set __position column
                elif key_str.startswith('po'):
                    new_dict['__position'] = value_str.upper()

                # Set __b and __t column
                elif (key_str.startswith('b')) & (not key_str.startswith('bi')):
                    new_dict['__b'] = value_str[0].upper()
                    if 'T' in key:
                        new_dict['__t'] = value_str[-1].upper()
                elif (key_str == 't') | (key_str.startswith('throw')) | (key_str.startswith('t/')):
                    new_dict['__t'] = value_str[0].upper()
                    if 'B' in key:
                        new_dict['__b'] = value_str[-1].upper()

                # Inlcude __ keys
                elif key_str.startswith('__'):
                    if key == '__division':
                        new_dict[key_str] = format_player_division(value_str)
                    elif key == '__hometown':
                        new_dict[key_str] = format_player_hometown(value_str)
                    else:
                        new_dict[key_str] = value_str

                # Set __obj column if no useful keys
                elif (key_str == '0') | (key_str == 'Unnamed: 0'):
                    new_dict['__obj'] = ' --- '.join(dictionary.values())
        # Combine fist and last name if necessary
        if (new_dict['__name'] == '') & (new_dict['_first_name'] != '') & (new_dict['_last_name'] != ''):
            new_dict['__name'] = new_dict['_first_name'] + ' ' + new_dict['_last_name']
        if (new_dict['__school'] != '') & (new_dict['__name'] != new_dict['__hometown']):
            new_dict_list.append(new_dict)
    
    canadians_df = pd.DataFrame(new_dict_list)
    canadians_df = canadians_df.loc[:, canadians_df.columns.str.startswith('__')]
    canadians_df.columns = canadians_df.columns.str.lstrip('__')
    return canadians_df[cols]


def generate_html(df, file_name, last_run):
    pd.set_option('colheader_justify', 'center')

    html_string = '''
    <html>
       <head>
          <title>2021 Canadians in College</title>
       </head>
       <header>
          <h1>2021 Canadians in College</h1>
          <h2>{number_of_players} Canadian players</h2>
          <h5>{last_run}</h5>
       </header>
       <link rel="stylesheet" type="text/css" href="df_style.css"/>
       <body>
    '''.format(number_of_players = str(len(df.index)), last_run = last_run)
    
    for division in ['NCAA: Division 1', 'NCAA: Division 2', 'NCAA: Division 3', 'NAIA',
                     'Junior Colleges and Community Colleges: Division 1',
                     'Junior Colleges and Community Colleges: Division 2',
                     'Junior Colleges and Community Colleges: Division 3']:
        temp_df = df[df['division'] == division].drop(['division'], axis=1).sort_values(by=['school', 'name'])
        html_string += '''
        <h2>{division}</h2>
        <h4>{number_of_players} players</h4>
        {table}
        '''.format(division = division, number_of_players = str(len(temp_df.index)), table = temp_df.to_html(na_rep = '', index = False, classes='mystyle'))

    html_string += '''
       </body>
    </html>.
    '''

    with open(file_name, 'w') as f:
        f.write(html_string)


def csv_to_dict_list(csv_file):
    with open(csv_file) as f:
        return [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]


# Run main function
if __name__ == "__main__":
    main()