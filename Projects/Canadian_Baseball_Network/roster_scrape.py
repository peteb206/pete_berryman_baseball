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
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import sys
import os
import ssl
import pytz
ssl._create_default_https_context = ssl._create_unverified_context


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
    full_run = True
    if len(sys.argv) == 1:
        schools_df = get_input(schools_df)
    elif sys.argv[1] == 'n': # Determine if running full web scraper or just updating google sheet
        full_run = False

    # Last run:
    last_run = 'Last updated: {} UTC'.format(datetime.datetime.now(pytz.utc).strftime("%B %d, %Y at %I:%M %p"))
    logger.info('')
    logger.info(last_run)

    canadians_df = pd.read_csv('canadians.csv') # Initialize canadians_df
    if full_run == True: # Run full web scraper
        # Set criteria for "Canadian" player search
        global city_strings, province_strings, country_strings, canada_strings, hometown_conversion_dict, ignore_strings
        city_strings, province_strings, country_strings, canada_strings, hometown_conversion_dict, ignore_strings = set_canadian_search_criteria()

        df_lists = iterate_over_schools(schools_df) # Iterate over schools
        canadians_dict_list = df_lists[1]

        # print_cols(roster_df_list) # Periodically check all of the table columns found in the html to see if we are overlooking anything

        canadians_df = format_df(canadians_dict_list, schools_df) # Format dictionaries to dataframe
        canadians_df = pd.concat([pd.read_csv('canadians_manual.csv'), canadians_df], ignore_index=True) # Add players who could not be scraped
        canadians_df.drop_duplicates(subset=['name', 'hometown'], keep='first', ignore_index=True, inplace=True) # Drop duplicate names (keep manually added rows if there is an "identical" scraped row)

        canadians_df['class'].fillna('Freshman', inplace=True)
        canadians_df['class'] = pd.Categorical(canadians_df['class'], ['Freshman','Sophomore', 'Junior', 'Senior', '']) # Create custom sort by class
        canadians_df['last_name'] = canadians_df['name'].str.split(' ').str[1]
        canadians_df = canadians_df.sort_values(by=['class', 'last_name', 'school'], ignore_index=True).drop('last_name', axis=1)

        canadians_df.to_csv('canadians.csv', index=False) # Export to canadians.csv as a reference

    canadians_df = canadians_df[['name','position','class','school','division','state','hometown']] # Keep only relevant columns

    update_gsheet(canadians_df, last_run) # Update Google Sheet

    generate_html(canadians_df, 'canadians.html', last_run) # Generate HTML with DataTables

    logger.info('')
    logger.info('{} Canadian players found...'.format(str(len(canadians_df.index))))


def read_roster_norm(html, school):
    df = html[0]
    if school['title'] not in ['Arizona Christian University', 'University of Northwestern Ohio']: # Has a real roster and a prospect/JV roster... ensure real roster is chosen
        for temp_df in html:
            if len(temp_df.index) > len(df.index): # Assume largest table on page is actual roster
                df = temp_df
    if df.columns.dtype == 'int64':
        new_header = df.iloc[0] # grab the first row for the header
        df = df[1:] # take the data less the header row
        df.columns = new_header # set the header row as the df header
    elif school['title'] in ['Mineral Area', 'Cowley']: # Columns in HTML table are messed up... keep an eye on these schools to see if fixed
        df.columns = ['Ignore', 'No.', 'Name', 'Pos.', 'B/T', 'Year', 'Ht.', 'Wt.', 'Hometown']
    df['__school'] = school['title']
    df['__division'] = school['division']
    df['__state'] = school['state']
    return df


def read_roster(school, header):
    response = ''
    try_num = 1
    while try_num <= 3: # 3 tries
        try:
            return read_roster_norm(pd.read_html(school['roster_link']), school)
        except Exception:
            try_num = try_num # Do nothing
        try:
            response = requests.get(school['roster_link'], headers=header, timeout=10)
            return read_roster_norm(pd.read_html(response.text), school)
        except Exception as e2:
            if try_num == 1:
                logger.info('')
            logger.info('--- e2: {} ---'.format(str(e2)))
        if try_num == 3:
            try:
                return str(response.text)
            except Exception as e3:
                logger.info('--- e3: {} ---'.format(str(e3)))
        logger.info('--- Failed attempt {} at {} ---'.format(str(try_num), school['roster_link']))
        try_num += 1
        time.sleep(0.5)
    return ''


def filter_canadians(df, canada_strings):
    out_list = list()
    roster_dict = df.to_dict(orient='records')
    index = 0
    while index < len(roster_dict):
        player = roster_dict[index]
        if 'Province' in player.keys(): # Applies to Canadian university
            player['__hometown'] = player['Hometown/High School'].split(':')[-1].split('/')[0].strip() + ', ' + player['Province'].split(':')[-1].strip()
            out_list.append(player)
        else: # Iterate over roster columns
            for attr in player:
                value = str(player[attr]).strip()
                if (any(canada_string.lower() in value.lower() for canada_string in canada_strings)) & (~any(ignore_string in value.lower() for ignore_string in ignore_strings)):
                    player['__hometown'] = value
                    out_list.append(player)
                    break
        index += 1
    return out_list


def set_canadian_search_criteria():
    city_strings = {
        'Quebec': ['montreal', 'saint-hilaire']
    }
    province_strings = {
        'Alberta': ['alberta', ', alta.', ', ab', 'a.b.'],
        'British Columbia': ['british columbia', ', b.c', ', bc'],
        'Manitoba': ['manitoba', ', mb', ', man.'],
        'New Brunswick': ['new brunswick', ', nb', 'n.b.'],
        'Newfoundland': ['newfoundland', 'nfld'],
        'Nova Scotia': ['nova scotia', ', ns', 'n.s.' ],
        'Ontario': [', ontario', ', on', ',on', '(ont)'],
        'Prince Edward Island': ['prince edward island', 'p.e.i.'],
        'Quebec': ['quebec', 'q.c.', ', qu'],
        'Saskatchewan': ['saskatchewan', ', sask', ', sk', 's.k.']
    }
    country_strings = {
        'Canada': ['canada', ', can']
    }
    canada_strings = list(sum(city_strings.values(), []))
    canada_strings.extend(sum(province_strings.values(), []))
    canada_strings.extend(sum(country_strings.values(), []))
    hometown_conversion_dict = {
        'ab': 'Alberta',
        'bc': 'British Columbia',
        'mb': 'Manitoba',
        'nb': 'New Brunswick',
        'ns': 'Nova Scotia',
        'on': 'Ontario',
        'ont': 'Ontario',
        'ont.': 'Ontario',
        'pei': 'Ontario',
        'qc': 'Quebec',
        'qu': 'Quebec',
        'sk': 'Saskatchewan'
    }
    for province, strings in province_strings.items():
         for string in strings:
                hometown_conversion_dict[re.sub(r'[^a-zA-Z]+', '', string)] = province
    ignore_strings =  ['canada college', 'west canada valley', 'la canada', 'australia', 'mexico', 'abac', 'newfoundland, pa', 'canada, minn', 'new brunswick, n']
    return city_strings, province_strings, country_strings, canada_strings, hometown_conversion_dict, ignore_strings


def get_input(schools_df):
    df_size = len(schools_df.index)
    run_all_rows = ''
    while run_all_rows not in ['y', 'n']:
        run_all_rows = input("\nRun script for all schools? Answer y/n... ")
        if run_all_rows not in ['y', 'n']:
            logger.error('Value must be "y" or "n"')
    if run_all_rows == 'n':
        start_row = '-1'
        while (int(start_row) < 0) | (int(start_row) >= df_size):
            start_row = input("\nStart with which row (>= 0 and <= {})... ".format(str(df_size - 1)))
            if int(start_row) < 0: 
                logger.error('Value must be greater than 0.')
            elif int(start_row) >= df_size:
                logger.error('Value must be less than or equal to {}.'.format(str(df_size - 1)))
        end_row = '-1'
        while (int(end_row) < 0) | (int(end_row) >= df_size) | (int(end_row) < int(start_row)):
            end_row = input("\nEnd with which row (>= 0 and <= {})... ".format(str(df_size - 1)))
            if int(end_row) < 0: 
                logger.error('Value must be greater than 0.')
            elif int(end_row) >= df_size:
                logger.error('Value must be less than or equal to {}.'.format(str(df_size - 1)))
            elif int(end_row) < int(start_row):
                logger.error('End row ({}) cannot be less than start row ({})'.format(end_row, start_row))
        schools_df = schools_df.loc[int(start_row):int(end_row)]
    return schools_df


def iterate_over_schools(schools_df):
    # Start timer
    start_time = time.time()

    # Print helpful info
    index_col_length, title_col_length, players_col_length, canadians_col_length, roster_link_col_length = 6, 52, 9, 11, 80
    logger.info('')
    logger.info('Reading the rosters of {} schools...'.format(str(len(schools_df.index))))
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
    elif ('f' in string.lower()) | ('1' in string) | ('hs' in string.lower()) | (string.lower() == 'rs.'):
        return 'Freshman'
    elif ('sr' in string.lower()) | ('gr' in string.lower()) | ('4' in string) | ('5' in string):
        return 'Senior'
    return string


def format_player_position(string):
    # Ouput position(s) in acronym form separated by a forward slash
    substitutions = {' ': '', 'PITCHER': 'P', 'RIGHT': 'R', 'LEFT': 'L', 'HANDED': 'H', '-H': 'H', 'THP': 'HP',
                     'CATCHER': 'C', 'UTILITY': 'UTL', 'FIRSTBASE': '1B', 'SECONDBASE': '2B', 'THIRDBASE': '3B',
                     'SHORTSTOP': 'SS', 'INFIELD': 'IF', 'OUTFIELD': 'OF', 'ER': '', 'MAN': '', ',R/R': ''}
    for from_string, to_string in substitutions.items():
        string = string.replace(from_string, to_string)
    return string


def format_player_division(string):
    level = 'Division ' + string[-1]
    if string.upper() == 'NAIA':
        return string.upper()
    elif 'JUCO' in string.upper():
        return 'Junior Colleges and Community Colleges: ' + level
    elif string.upper() == 'CCCAA':
        return 'California Community College Athletic Association'
    elif string.upper() == 'NWAC':
        return 'Northwest Athletic Conference'
    elif string.upper() == 'USCAA':
        return 'United States Collegiate Athletic Association'
    else:
        return 'NCAA: ' + level


def format_player_hometown(string):
    string = re.sub(r'\s*\(*(?:Canada|Can.|CN|CAN|CA)\)*\.*', '', string) # Remove references to Canada

    parentheses_search = re.search(r'\(([^)]+)', string) # Search for text within parentheses
    if parentheses_search != None:
        if parentheses_search.group(1).count(',') == 1:
            string = parentheses_search.group(1) # Text within parentheses is city/province
        else:
            string = string.split('(')[0].strip() # Text within parentheses is not helpful

    comma_count = string.count(',')
    no_school = string.split('/')[0].strip()
    if comma_count > 0:
        no_school_list = no_school.split(',')
        city = no_school_list[0].strip()
        province = no_school_list[1].strip()
        province_stripped = re.sub(r'[^a-zA-Z]+', '', province.lower()) # Ex. ", Sask." --> "sask"
        if province_stripped in hometown_conversion_dict.keys():
            province = hometown_conversion_dict[province_stripped] # Convert province abbreviations to full name
        elif (province[:3].lower() == 'can') | (province == ''):
            if city.strip().lower() in hometown_conversion_dict.keys():
                city = hometown_conversion_dict[city.strip().lower()] # In case province accidentally labeled as city
            return city # No province provided... just return city
        string = city + ', ' + province
    else:
        string = no_school
    return string


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
        hometown_orig = ''

        for key, value in dictionary.items():
            key_str = str(key).lower()
            value_str = str(value)
            value_str = value_str.split(':')[-1].strip()
            if (len(value_str) != 0) & (value_str.lower() != 'nan'):
                # Set __class column
                if (new_dict['__class'] == '') & (key_str.startswith('cl') | key_str.startswith('y') | key_str.startswith('e') |
                                                  key_str.startswith('ci.') | ('year' in key_str) | (key_str in ['athletic', 'academic'])):
                    new_dict['__class'] = format_player_class(value_str)

                # Set __name columns
                elif ('first' in key_str) & ('last' not in key_str):
                    new_dict['_first_name'] = value_str
                elif (key_str == 'last') | (('last' in key_str) & ('nam' in key_str)):
                    new_dict['_last_name'] = value_str
                elif ('name' in key_str) | (key_str == 'player') | (key_str == 'student athlete'):
                    new_dict['__name'] = format_player_name(value_str) # Format as "First Last"

                # Set __position column
                elif key_str.startswith('po'):
                    new_dict['__position'] = format_player_position(value_str.upper())

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
                        hometown_orig = value_str
                        new_dict[key_str] = format_player_hometown(value_str)
                    else:
                        new_dict[key_str] = value_str

                # Set __obj column if no useful keys
                elif (key_str == '0') | (key_str == 'unnamed: 0'):
                    new_dict['__obj'] = ' --- '.join(dictionary.values())
        # Specify RHP/LHP for pitcher
        if ('P' in new_dict['__position']) & ('HP' not in new_dict['__position']) & (new_dict['__t'] != ''):
            new_dict['__position'] = new_dict['__position'].replace('P', new_dict['__t'] + 'HP')
        # Combine fist and last name if necessary
        if (new_dict['__name'] == '') & (new_dict['_first_name'] != '') & (new_dict['_last_name'] != ''):
            new_dict['__name'] = new_dict['_first_name'] + ' ' + new_dict['_last_name']
        elif ('Name' in dictionary.keys()) & ('Name.1' in dictionary.keys()):
            new_dict['__name'] = dictionary['Name'] + ' ' + dictionary['Name.1']
        if (new_dict['__name'] != '') & (new_dict['__school'] != hometown_orig) & (new_dict['__name'] != hometown_orig):
            new_dict['__name'] = re.sub(r'\s+', ' ', new_dict['__name']) # Remove unnecessary spaces in names
            new_dict_list.append(new_dict)

    canadians_df = pd.DataFrame(new_dict_list)
    canadians_df = canadians_df.loc[:, canadians_df.columns.str.startswith('__')]
    canadians_df.columns = canadians_df.columns.str.lstrip('__')
    return canadians_df[cols]


def generate_html(df, file_name, last_run):
    pd.set_option('colheader_justify', 'center')

    html_string = '''
<!DOCTYPE HTML>
<html>
    <head>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.23/css/jquery.dataTables.css">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/buttons/1.6.5/css/buttons.dataTables.min.css">
        <script type="text/javascript" charset="utf8" src="https://code.jquery.com/jquery-3.5.1.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/1.10.23/js/jquery.dataTables.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/1.6.5/js/dataTables.buttons.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.1.3/jszip.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/pdfmake.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/vfs_fonts.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/1.6.5/js/buttons.html5.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/1.6.5/js/buttons.print.min.js"></script>
        <script type="text/javascript">
            $(document).ready(function () {{
                $('table.display').DataTable({{
                    'lengthMenu': [[10, 25, 50, 100, -1], [10, 25, 50, 100, 'All']],
                    'pageLength': -1,
                    'order': [[2, 'asc'], [0, 'asc'], [3, 'asc']],
                    'dom': 'Bflrtip'
                }})
            }});
        </script>
        <title>2021 Canadians in College</title>
    </head>
    <header>
        <h1 style='text-align: center'>2021 Canadians in College</h1>
        <h2 style='text-align: center'>{number_of_players} Canadian players</h2>
        <h5 style='text-align: center'>{last_run}</h5>
    </header>
    <body>
    '''.format(number_of_players = str(len(df.index)), last_run = last_run)

    iter = 0
    for division in [
        'NCAA: Division 1', 'NCAA: Division 2', 'NCAA: Division 3', 'NAIA',
        'Junior Colleges and Community Colleges: Division 1',
        'Junior Colleges and Community Colleges: Division 2',
        'Junior Colleges and Community Colleges: Division 3',
        'California Community College Athletic Association',
        'Northwest Athletic Conference',
        'United States Collegiate Athletic Association'
    ]:
        temp_df = df[df['division'] == division].drop(['division'], axis=1)
        html_string += '''
        <div style='padding:3%'>
            <h2 style='text-align: center'>{division}</h2>
            {table}
        </div>
        '''.format(division = division,
                   number_of_players = str(len(temp_df.index)),
                   table = temp_df.to_html(na_rep = '', index=False, classes='display" id="table{}'.format(str(iter))))
        iter += 1

    html_string += '''
    </body>
</html>.
    '''

    # Make cells editable
    # html_string = html_string.replace('<td>', "<td contenteditable='true'>")

    # Custom sort order for class: Freshman, Sophomore, Junior, Senior
    for class_year, class_year_num in {'Freshman': '1', 'Sophomore': '2', 'Junior': '3', 'Senior': '4'}.items():
        html_string = html_string.replace('>{}<'.format(class_year), " data-order='{}'>{}<".format(class_year_num, class_year))

    with open(file_name, 'w') as f:
        f.write(html_string)


def update_gsheet(df, last_run):
    blank_row = [['', '', '', '', '']]

    # define the scope
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    # add credentials to the account
    keyfile_dict = json.loads(os.environ.get('KEYFILE'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict, scope)

    # authorize the clientsheet 
    client = gspread.authorize(creds)

    # get the instance of the Spreadsheet
    sheet = client.open(os.environ.get('SHEET_NAME'))

    # get the sheets of the Spreadsheet
    players_sheet = sheet.worksheet('2021')
    players_sheet_id = players_sheet._properties['sheetId']

    # clear values in both sheets
    clear_sheets(sheet, [players_sheet_id])

    # initialize summary data
    last_run_split = last_run.split(': ') + ['', '', '']
    last_run_split[0] = last_run_split[0] + ':'
    summary_data = [last_run_split] + blank_row + [['Total', '{} players'.format(str(len(df.index))), '', '', '']] + blank_row

    # Fill NaN values in dataframe with blank string
    df.fillna('', inplace=True)

    # Add title row
    col_headers = [df.drop(['division', 'class'], axis=1).columns.values.tolist()]
    player_data = list()

    division_header_rows = list()
    class_header_rows = list()

    division_list = ['NCAA: Division 1', 'NCAA: Division 2', 'NCAA: Division 3', 'NAIA', 'Junior Colleges and Community Colleges: Division 1', 'Junior Colleges and Community Colleges: Division 2', 'Junior Colleges and Community Colleges: Division 3', 'California Community College Athletic Association', 'Northwest Athletic Conference', 'United States Collegiate Athletic Association']
    class_list = ['Freshman', 'Sophomore', 'Junior', 'Senior']

    # Loop through divisions
    for division in division_list:

        # Row/Division Header
        player_data += [[division, '', '', '', '']]

        # Subset dataframe
        df_split_div = df[df['division'] == division].drop(['division'], axis=1)
        for class_year in class_list:
            df_split_class = pd.DataFrame()
            if class_year == 'Freshman':
                df_split_class = df_split_div[(df_split_div['class'] == class_year) | (df_split_div['class'] == '')].drop(['class'], axis=1)
                class_year = 'Freshmen'
            else:
                df_split_class = df_split_div[df_split_div['class'] == class_year].drop(['class'], axis=1)
                if len(df_split_class.index) > 0:
                    player_data += blank_row
                class_year += 's'
            if len(df_split_class.index) > 0:
                player_data += ([[class_year, '', '', '', '']] + col_headers + df_split_class.values.tolist())

        # Compile data rows
        player_data += blank_row
        summary_data.append([division + ' ', '{} players'.format(str(len(df_split_div.index))), '', '', ''])

    # Add data to sheets
    data = summary_data + blank_row + player_data
    players_sheet.insert_rows(data, row=1)

    # Resize columns and re-size sheets
    resize_columns(sheet, [players_sheet_id])
    players_sheet.resize(rows=len(data))

    # Format division/class headers
    format_headers(sheet, players_sheet_id, players_sheet.findall(re.compile(r'^(' + '|'.join(division_list) + r')$')), True)
    time.sleep(60) # break up the requests to avoid error
    format_headers(sheet, players_sheet_id, players_sheet.findall(re.compile(r'^(' + '|'.join(['Freshmen', 'Sophomores', 'Juniors', 'Seniors']) + r')$')), False)
    players_sheet.format('A3:A15', {'textFormat': {'bold': True}}) # bold Summary text
    players_sheet.format('B2:B2', {'backgroundColor': {'red': 1, 'green': 0.91, 'blue': 0.47}}) # background color
    players_sheet.format('A3:E3', {'backgroundColor': {'red': 0.92, 'green': 0.92, 'blue': 0.92}}) # background color
    players_sheet.format('A16:E{}'.format(len(data)), {'horizontalAlignment': 'CENTER', 'verticalAlignment': 'MIDDLE'}) # center all cells

    logger.info('Google sheet updated with {} players...'.format(str(len(df.index))))


def clear_sheets(spreadsheet, sheet_ids):
    body = dict()
    requests = list()
    for sheet_id in sheet_ids:
        request = dict()
        update_cells_dict = dict()
        range_dict = dict()
        range_dict['sheetId'] = sheet_id
        update_cells_dict['range'] = range_dict
        update_cells_dict['fields'] = '*'
        request['updateCells'] = update_cells_dict
        requests.append(request)
    body['requests'] = requests
    spreadsheet.batch_update(body)


def resize_columns(spreadsheet, sheet_ids):
    body = dict()
    requests = list()
    sheet_num = 1
    for sheet_id in sheet_ids:
        request = dict()
        auto_resize_dimensions = dict()
        dimensions = dict()
        dimensions['sheetId'] = sheet_id
        dimensions['dimension'] = 'COLUMNS'
        dimensions['startIndex'] = 0 if sheet_num == 1 else 1
        dimensions['endIndex'] = 2 if sheet_num == 1 else 5
        auto_resize_dimensions['dimensions'] = dimensions
        request['autoResizeDimensions'] = auto_resize_dimensions
        if sheet_num == 1:
            requests.append(request)
        else:
            requests.append(request)
            request2 = dict()
            update_dimension_properties = dict()
            range_dict = dict()
            range_dict['sheetId'] = sheet_id
            range_dict['dimension'] = 'COLUMNS'
            range_dict['startIndex'] = 0
            range_dict['endIndex'] = 1
            properties_dict = dict()
            properties_dict['pixelSize'] = 350
            update_dimension_properties['range'] = range_dict
            update_dimension_properties['properties'] = properties_dict
            update_dimension_properties['fields'] = 'pixelSize'
            request2['updateDimensionProperties'] = update_dimension_properties
            requests.append(request2)
        sheet_num += 1
    body['requests'] = requests
    spreadsheet.batch_update(body)


def format_headers(spreadsheet, sheet_id, occurrences, division_header):
    color = 0.8
    font_size = 20
    if division_header == False:
        color = 0.92
        font_size = 14

    range = {
        'sheetId': sheet_id,
        'startColumnIndex': 0,
        'endColumnIndex': 5
    }

    body = dict()
    requests = list()
    for occurrence in occurrences:
        row = occurrence.row
        # merge cells and format header
        range['startRowIndex'] = row - 1
        range['endRowIndex'] = row
        requests += [
            {
                'mergeCells': {
                    'mergeType': 'MERGE_ALL',
                    'range': range
                }
            }, {
                'repeatCell': {
                    'range': range,
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {
                                'red': color,
                                'green': color,
                                'blue': color
                            },
                            'textFormat': {
                                'fontSize': font_size,
                                'bold': True
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat)',
                }
            }
        ]
        body['requests'] = requests
        spreadsheet.batch_update(body)
        if division_header == False:
            # format column headers
            range['startRowIndex'] = row
            range['endRowIndex'] = row + 1
            body['requests'] = [
                {
                    'repeatCell': {
                        'range': range,
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat)',
                    }
                }
            ]
            spreadsheet.batch_update(body)


def csv_to_dict_list(csv_file):
    with open(csv_file) as f:
        return [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]


# Run main function
if __name__ == "__main__":
    main()