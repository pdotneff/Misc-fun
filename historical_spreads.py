import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
import re
from time import sleep


def import_data():
    past_data = pd.read_csv("NBAGames_1718.csv", index_col="date")
    past_data.index = past_data.index.map(lambda x: datetime.strptime(x, '%m/%d/%y'))
    historical_dates = sorted(
        list(set(past_data.index.map(lambda x: datetime.strftime(x, '%Y-%m-%d')))))
    historical_dates = ["".join(x.split('-')) for x in historical_dates]

    return past_data.sort_index(), historical_dates


def create_hist_pt_spread_table(webpage, date=None):
    soup = bs(webpage.content, 'html.parser')

    teams = []
    vegas_spread = []

    table_data = soup.select("tr")
    team_data = soup.find_all("span", class_='oddsTeamWLink')

    for row in table_data[2:]:
        try:
            data = row.select("td")
            # 7th index should spit back data for the 'Mirage' which tends
            # to be in line with Pinnacle
            data = data[7].get_text().split('\n')
            vegas_spread.append(data[0])
            if data[0] == '--':
                vegas_spread.append(data[0])
            else:
                other_spread = re.search(r'([A-Za-z]+|[+-][.0-9]+)$', data[1].strip())
                vegas_spread.append(other_spread[0])
        except Exception as e:
            pass

    for team in team_data:
        teams.append(team.get_text())

    print(teams)
    print(vegas_spread)
    print(len(teams), len(vegas_spread))

    if date != None:
        date = date[:4] + '-' + date[4:6] + '-' + date[6:]
        date = datetime.strptime(date, '%Y-%m-%d')

    pt_spread_table = pd.DataFrame({'teams': teams, 'veg_spread': vegas_spread})
    pt_spread_table["date"] = date
    pt_spread_table = pt_spread_table.set_index("date")

    return pt_spread_table


def get_hist_spreads(dates):
    page = "http://www.donbest.com/nba/odds/spreads/"

    team_dict = {'Atlanta Hawks': 'Atlanta', 'Boston Celtics': 'Boston',
                 'Brooklyn Nets': 'Brooklyn', 'Charlotte Hornets': 'Charlotte',
                 'Chicago Bulls': 'Chicago', 'Cleveland Cavaliers': 'Cleveland',
                 'Dallas Mavericks': 'Dallas', 'Denver Nuggets': 'Denver',
                 'Detroit Pistons': 'Detroit', 'Golden State Warriors': 'Golden State',
                 'Houston Rockets': 'Houston', 'Indiana Pacers': 'Indiana',
                 'Los Angeles Clippers': 'LA Clippers', 'Los Angeles Lakers': 'LA Lakers',
                 'Memphis Grizzlies': 'Memphis', 'Miami Heat': 'Miami',
                 'Milwaukee Bucks': 'Milwaukee', 'Minnesota Timberwolves': 'Minnesota',
                 'New Orleans Pelicans': 'New Orleans', 'New York Knicks': 'New York',
                 'Oklahoma City Thunder': 'Okla City', 'Orlando Magic': 'Orlando',
                 'Philadelphia 76ers': 'Philadelphia', 'Phoenix Suns': 'Phoenix',
                 'Portland Trail Blazers': 'Portland', 'Sacramento Kings': 'Sacramento',
                 'San Antonio Spurs': 'San Antonio', 'Toronto Raptors': 'Toronto',
                 'Utah Jazz': 'Utah', 'Washington Wizards': 'Washington'}

    count = 0
    BRIDGE = '.html'
    final_table = pd.DataFrame()

    for date in dates:
        count += 1
        URL = page + date + BRIDGE
        webpage = requests.get(URL)
        sleep(6)
        temp_table = create_hist_pt_spread_table(webpage, date)

        if len(final_table) == 0:
            final_table = temp_table
        else:
            final_table = pd.concat([final_table, temp_table])

        if count % 5 == 0:
            print("Count is {}".format(count))

    final_table['teams'] = final_table['teams'].map(team_dict)

    return final_table


if __name__ == "__main__":
    start = datetime.now()
    nba_df, old_dates = import_data()
    output = get_hist_spreads(old_dates)

    with open("vegas_spreads.csv", 'a') as f:
        output.to_csv(f, header=f.tell() == 0)
    end = datetime.now()
    print(end - start)
