import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


def get_canadian_players(year):
    # assert type(year) = int
    page = requests.get('https://www.canadianbaseballnetwork.com/canadians-in-college/{}-canadians-in-college'.format(str(year)))
    soup = BeautifulSoup(page.text, 'html.parser')
    div = soup.find("div", {"id": "block-yui_3_17_2_1_1604547079964_31164"})
    df_dict = dict()
    class_list = ['freshmen', 'sophomores', 'juniors', 'seniors']
    for year in class_list:
         df_dict[year] = list()
    class_list.append('freshman')

    current_class = ''
    for p in div.find_all("p"):
        text = p.getText()
        if (current_class != ''):
            for m in re.compile("\(.{2}\)").finditer(text):
                text = text[0: m.end()]
                #position = text.split(' ')[0]
                player_dict = dict()
                player_dict['text'] = text
                df_dict[current_class if current_class != 'freshman' else 'freshmen'].append(player_dict)
        if text.lower() in class_list:
            current_class = text.lower()

    class_list.remove('freshman')
    for class_value in class_list:
        print('{}: {} people'.format(year, str(len(df_dict[class_value]))))