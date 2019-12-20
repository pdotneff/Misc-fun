import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime


def import_data():
    past_data = pd.read_csv("NBAGames_20190120.csv", index_col="date")
    past_data.index = past_data.index.map(lambda x: datetime.strptime(x, '%m/%d/%y'))
    historical_dates = set(past_data.index.map(lambda x: datetime.strftime(x, '%Y-%m-%d')))

    return past_data.sort_index(), list(historical_dates)


def create_table_precise(webpage, head_text=None):
    soup = bs(webpage.content, 'html.parser')

    teams = []
    home = []
    away = []

    # Select 'tr' tags from 'tbody'
    table_data = soup.select("tbody tr")

    # Separate table_data items into individual 'td' tags so you can access tags directly
    table_sub_data = [item.find_all("td") for item in table_data]

    # Create list of entire team data > need to access 'data-sort' attr. like a dictionary
    clean_data = []
    for item in table_sub_data:
        temp = []
        for sub_item in item:
            temp.append(sub_item['data-sort'])
        clean_data.append(temp)

    for row in clean_data:
        teams.append(row[1])
        home.append(row[5])
        away.append(row[6])

    final_home_table = pd.DataFrame({"teams": teams,
                                     "{}_home".format(head_text): home})
    final_away_table = pd.DataFrame({"teams": teams,
                                     "{}_away".format(head_text): away}, )

    final_home_table = final_home_table.set_index("teams")
    final_away_table = final_away_table.set_index("teams")

    return final_home_table, final_away_table


def get_stats(data_df, historical_dates):
    pages = {"https://www.teamrankings.com/nba/stat/offensive-efficiency": "OEff",
             "https://www.teamrankings.com/nba/stat/fastbreak-efficiency": "F/BEff",
             "https://www.teamrankings.com/nba/stat/average-scoring-margin": "ASM",
             "https://www.teamrankings.com/nba/stat/true-shooting-percentage": "TSP",
             "https://www.teamrankings.com/nba/stat/offensive-rebounding-pct": "OReb%",
             "https://www.teamrankings.com/nba/stat/defensive-rebounding-pct": "DReb%",
             "https://www.teamrankings.com/nba/stat/block-pct": "Block%",
             "https://www.teamrankings.com/nba/stat/assist--per--turnover-ratio": "ATRat",
             "https://www.teamrankings.com/nba/stat/defensive-efficiency": "DEff",
             "https://www.teamrankings.com/nba/stat/opponent-true-shooting-percentage": "OTSP",
             "https://www.teamrankings.com/nba/stat/opponent-steals-per-game": "OSPG",
             "https://www.teamrankings.com/nba/stat/opponent-personal-fouls-per-game": "OP/FPG",
             "https://www.teamrankings.com/nba/stat/percent-of-points-from-free-throws": "Frees%",
             "https://www.teamrankings.com/nba/stat/fta-per-fga": "FTAFGA%",
             "https://www.teamrankings.com/nba/stat/steal-pct": "SPDP%",
             "https://www.teamrankings.com/nba/stat/assists-per-fgm": "AsstpFGM",
             "https://www.teamrankings.com/nba/stat/personal-foul-pct": "Foul%",
             "https://www.teamrankings.com/nba/stat/opponent-fta-per-fga": "OFTAFGA",
             "https://www.teamrankings.com/nba/stat/turnover-pct": "TpOP",
             "https://www.teamrankings.com/nba/stat/opponent-two-point-rate": "O2pt%",
             "https://www.teamrankings.com/nba/stat/possessions-per-game": "PossPG",
             "https://www.teamrankings.com/nba/stat/average-4th-quarter-margin": "Av4QM"}

    count = 0
    BRIDGE = "?date="
    final_table = pd.DataFrame()

    for date in sorted(historical_dates):
        count += 1
        end_home_table = pd.DataFrame()
        end_away_table = pd.DataFrame()
        temp_df = data_df.loc[date]
        visit_teams = list(temp_df['visitor'])
        home_teams = list(temp_df['home'])

        for page, head in pages.items():
            URL = page + BRIDGE + date
            webpage = requests.get(URL)
            home_table, away_table = create_table_precise(webpage, head)

            try:
                end_home_table.index > 0
                end_home_table = home_table
                end_away_table = away_table
            except:
                end_home_table = end_home_table.merge(home_table, left_index=True,
                                                      right_index=True)
                end_away_table = end_away_table.merge(away_table, left_index=True,
                                                      right_index=True)

        try:
            temp_df = temp_df.merge(end_away_table, how="inner", left_on='visitor',
                                    right_index=True)
            temp_df = temp_df.merge(end_home_table, how="inner", left_on='home', right_index=True)
        except:
            temp_df = pd.DataFrame(temp_df).transpose()
            temp_df = temp_df.merge(end_away_table, how="inner", left_on='visitor',
                                    right_index=True)
            temp_df = temp_df.merge(end_home_table, how="inner", left_on='home', right_index=True)

        if len(final_table) == 0:
            final_table = temp_df
        else:
            final_table = pd.concat([final_table, temp_df])

    return final_table


if __name__ == "__main__":
    start = datetime.now()
    nba_df, old_dates = import_data()
    output = get_stats(nba_df, old_dates)

    df_start = list(output.columns[:2])
    df_end = list(sorted(output.columns[2:]))
    order = df_start + df_end

    output = output[order]

    with open("NBA_Data_Current.csv", 'a') as f:
        output.to_csv(f, header=f.tell() == 0)
    end = datetime.now()
    print(end - start)
