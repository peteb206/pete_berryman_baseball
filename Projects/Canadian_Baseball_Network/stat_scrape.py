import requests
import pandas as pd


def main():
    players_df = pd.read_csv('canadians.csv')

    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    for index, player in players_df.iterrows():
        if (player['stats_link'] != '') & (type(player['stats_link']) == type('')):
            for url in player['stats_link'].split(','):
                df = scrape_stats(url, player['division'], header)
                if len(df.index) > 0:
                    print(df.columns.tolist())


def scrape_stats(url, division, header):
    table_index = 2
    reset_header = False
    if division.startswith('NCAA'):
        table_index = 4
        reset_header = True

    response = requests.get(url, headers=header, timeout=15).text

    df = pd.DataFrame()
    try:
        dfs = pd.read_html(response)
        df = dfs[0]
        for temp_df in dfs:
            if len(temp_df.index) > len(df.index): # Assume largest table on page is gamelog
                df = temp_df
        if reset_header == True:
            new_header = df.iloc[1] # grab the first row for the header
            df = df[2:] # take the data less the header row
            df.columns = new_header # set the header row as the df header
    except Exception as e:
        print(str(e))
    return df


# Run main function
if __name__ == "__main__":
    main()