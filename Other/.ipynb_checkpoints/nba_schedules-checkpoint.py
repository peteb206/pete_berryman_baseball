import pandas as pd

# Import the NBA schedule
months = {'1': 'october', '2': 'november', '3': 'december', '4': 'january', '5': 'february', '6': 'march', '7': 'april'}
schedule = pd.DataFrame()

for month in months:
    temp = pd.read_html('https://www.basketball-reference.com/leagues/NBA_2020_games-{}.html'.format(months[month]))[0]
    if month == 1:
        schedule = temp
    else:
        schedule = pd.concat([schedule, temp], ignore_index = True)

schedule.rename(columns = {'Visitor/Neutral': 'Visitor', 'Home/Neutral': 'Home', 'PTS': 'PTS1', 'PTS.1': 'PTS2'}, inplace = True)

# Function to compare schedule overlap for teams
def team_compare(teams = 'all', sort_by_overlaps = False):
    if teams == 'all':
        teams = [
            'Atlanta Hawks',
            'Boston Celtics',
            'Brooklyn Nets',
            'Charlotte Hornets',
            'Chicago Bulls',
            'Cleveland Cavaliers',
            'Dallas Mavericks',
            'Denver Nuggets',
            'Detroit Pistons',
            'Golden State Warriors',
            'Houston Rockets',
            'Indiana Pacers',
            'Los Angeles Clippers',
            'Los Angeles Lakers',
            'Memphis Grizzlies',
            'Miami Heat',
            'Milwaukee Bucks',
            'Minnesota Timberwolves',
            'New Orleans Pelicans',
            'New York Knicks',
            'Oklahoma City Thunder',
            'Orlando Magic',
            'Philadelphia 76ers',
            'Phoenix Suns',
            'Portland Trail Blazers',
            'Sacramento Kings',
            'San Antonio Spurs',
            'Toronto Raptors',
            'Utah Jazz',
            'Washington Wizards'
        ]
    
    schedule_all = pd.DataFrame()
    
    for team in teams:
        query = "Visitor == '{}' or Home == '{}'".format(team, team)
        schedule_new = schedule.query(query)
        schedule_new.insert(1, column = "Team", value = team)
        schedule_new.insert(2, column = "Value", value = 1)
        if schedule_all.empty:
            schedule_all = schedule_new
        else:
            schedule_all = schedule_all.append(schedule_new)
            
    schedule_pivot = schedule_all.pivot(index = "Date", columns = "Team", values = "Value").fillna(0)
    schedule_pivot1 = schedule_pivot.stack().to_frame().reset_index("Team").rename(columns = {0:'Value'})
    schedule_pivot2 = schedule_pivot.stack().to_frame().reset_index("Team").rename(columns = {0:'Value'})
    
    schedule_merge = pd.merge(schedule_pivot1, schedule_pivot2, how = 'outer', on = ['Date', "Value"], suffixes = ['1', '2'])
    schedule_merge.query("Team1 != Team2 and Team1 < Team2 and Value > 0", inplace = True)
    schedule_merge.reset_index(inplace = True)
    schedule_merge.drop_duplicates(inplace = True)
    schedule_merge = schedule_merge.groupby(["Team1", "Team2"]).size().to_frame().reset_index().rename(columns = {0: 'Value'})
    
    if sort_by_overlaps is not False:
        print()
        print('NBA games overlapped, ordered by number of mutual gamedates') 
        print()
        schedule_merge["String"] = schedule_merge['Value'].map(str) + ' : ' + schedule_merge["Team1"] + ' - ' + schedule_merge['Team2']
        schedule_merge.sort_values(['Value'], ascending = False, inplace = True)
    else:
        print()
        print('NBA games overlapped, ordered by team name')
        print()
        schedule_merge["String"] = schedule_merge["Team1"] + ' - ' + schedule_merge['Team2'] + ' : ' + + schedule_merge['Value'].map(str) 
    
    schedule_merge = list(schedule_merge['String'])
	 
    for i in schedule_merge:
        print(i)