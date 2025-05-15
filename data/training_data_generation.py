from nba_api.stats.endpoints import playbyplayv2, boxscoretraditionalv2, teamgamelog, leaguegamefinder
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.static import teams
import pandas as pd
from tqdm import tqdm
import time


def get_team_id_by_name(team_name):
    all_teams = teams.get_teams()
    for team in all_teams:
        if team_name.lower() in team['abbreviation'].lower():
            return team['id'], team['nickname']
    return None

def get_past_games(season="2023-24", num_games=10):
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    games = gamefinder.get_data_frames()[0]
    games = games[games["MATCHUP"].str.contains("@")]
    return games.head(num_games)

def simulate_snapshots(game_id, game_date, home_team_name, away_team_name, interval=120):
    pbp = playbyplayv2.PlayByPlayV2(game_id=game_id).get_data_frames()[0]
    time.sleep(0.5)

    home_team_id, home_team_nickname = get_team_id_by_name(home_team_name)
    away_team_id, away_team_nickname = get_team_id_by_name(away_team_name.strip())
    time.sleep(0.5)

    home_log = teamgamelog.TeamGameLog(team_id=home_team_id, season='2023').get_data_frames()[0]
    away_log = teamgamelog.TeamGameLog(team_id=away_team_id, season='2023').get_data_frames()[0]
    home_before = home_log[pd.to_datetime(home_log['GAME_DATE']) < game_date]
    away_before = away_log[pd.to_datetime(away_log['GAME_DATE']) < game_date]
    home_wins = (home_before['WL'] == 'W').sum()
    home_losses = (home_before['WL'] == 'L').sum()
    away_wins = (away_before['WL'] == 'W').sum()
    away_losses = (away_before['WL'] == 'L').sum()

    final_margin = pbp['SCOREMARGIN'].dropna().iloc[-1]
    label = 1 if int(final_margin) > 0 else 0

    # Accumulators for stats
    stats = {
        'home': {'fgm': 0, 'fga': 0, 'fg3m': 0, 'fg3a': 0, 'ftm': 0, 'fta': 0, 'to': 0, 'reb': 0},
        'away': {'fgm': 0, 'fga': 0, 'fg3m': 0, 'fg3a': 0, 'ftm': 0, 'fta': 0, 'to': 0, 'reb': 0}
    }

    snapshots = []
    current_time = 2880  # start of game in seconds
    last_snapshot_time = 2880

    for idx, row in pbp.iterrows():
        if not isinstance(row['PCTIMESTRING'], str):
            continue

        m, s = map(int, row['PCTIMESTRING'].split(':'))
        period = row['PERIOD']
        seconds_left = (4 - period) * 720 + m * 60 + s
        team_abbr = row['PLAYER1_TEAM_ABBREVIATION']
    
        if team_abbr == home_team_name or home_team_nickname in str(row['HOMEDESCRIPTION']):
            team = 'home'
        if team_abbr == away_team_name or home_team_nickname in str(row['VISITORDESCRIPTION']):
            team = 'away'
        msg_type = row['EVENTMSGTYPE']
        action_type = row['EVENTMSGACTIONTYPE']

        if msg_type == 1:  # Made FG
            stats[team]['fgm'] += 1
            stats[team]['fga'] += 1
            if action_type in [1, 2, 3]:  # 3PT actions
                stats[team]['fg3m'] += 1
                stats[team]['fg3a'] += 1
            else:
                stats[team]['fg3a'] += 0  # no increase
        elif msg_type == 2:  # Missed FG
            stats[team]['fga'] += 1
            if action_type in [1, 2, 3]:
                stats[team]['fg3a'] += 1
        elif msg_type == 3:  # Free throws
            stats[team]['fta'] += 1
            desc = str(row['HOMEDESCRIPTION']) + str(row['VISITORDESCRIPTION'])
            if 'miss' not in desc.lower():
                stats[team]['ftm'] += 1
        elif msg_type == 4:  # Rebound
            stats[team]['reb'] += 1
        elif msg_type == 5:  # Turnover
            stats[team]['to'] += 1
        else:
            pass

        if seconds_left <= last_snapshot_time - interval:
            score = row['SCORE']
            if isinstance(score, str) and '-' in score:
                home_score, away_score = map(int, score.split('-'))

                # Momentum: score 2 minutes ago
                target_time = seconds_left + interval
                prev_home, prev_away = home_score, away_score

                for j in range(idx, -1, -1):
                    prev_row = pbp.iloc[j]
                    if isinstance(prev_row['PCTIMESTRING'], str):
                        m2, s2 = map(int, prev_row['PCTIMESTRING'].split(':'))
                        prev_time = (4 - prev_row['PERIOD']) * 720 + m2 * 60 + s2
                        if prev_time >= target_time and isinstance(prev_row['SCORE'], str) and '-' in prev_row['SCORE']:
                            prev_home, prev_away = map(int, prev_row['SCORE'].split('-'))
                            break
                print(home_score, prev_home)
                print(away_score, prev_away)
                home_momentum = home_score - prev_home
                away_momentum = away_score - prev_away
                score_diff_momentum = (home_score - away_score) - (prev_home - prev_away)

                home = stats['home']
                away = stats['away']
    
                snapshot = {
                    'game_id': game_id,
                    'home_team_wins': home_wins,
                    'home_team_losses': home_losses,
                    'away_team_wins': away_wins,
                    'away_team_losses': away_losses,
                    'seconds_left': seconds_left,
                    'period': period,
                    'home_score': home_score,
                    'away_score': away_score,
                    'score_diff': home_score - away_score,
                    'home_fg_pct': "{:.3f}".format(home['fgm'] / home['fga']) if home['fga'] else 0.0,
                    'home_3pt_pct': "{:.3f}".format(home['fg3m'] / home['fg3a']) if home['fg3a'] else 0.0,
                    'home_ft_pct': "{:.3f}".format(home['ftm'] / home['fta']) if home['fta'] else 0.0,
                    'home_to': home['to'],
                    'home_reb': home['reb'],
                    'away_fg_pct': "{:.3f}".format(away['fgm'] / away['fga']) if away['fga'] else 0.0,
                    'away_3pt_pct': "{:.3f}".format(away['fg3m'] / away['fg3a']) if away['fg3a'] else 0.0,
                    'away_ft_pct': "{:.3f}".format(away['ftm'] / away['fta']) if away['fta'] else 0.0,
                    'away_to': away['to'],
                    'away_reb': away['reb'],
                    'score_diff_momentum': score_diff_momentum,
                    'points_scored_last_2min_home': home_momentum,
                    'points_scored_last_2min_away': away_momentum,
                    'label': label
                }
                snapshots.append(snapshot)
                last_snapshot_time = seconds_left

    return snapshots


def main():
    all_snapshots = []
    games = get_past_games()
    for _, game in tqdm(games.iterrows(), total=len(games)):
        game_id = game['GAME_ID']
        game_date = game['GAME_DATE']
        home_team = game["MATCHUP"][game["MATCHUP"].index("@")+2:].strip()
        away_team = game["MATCHUP"][:game["MATCHUP"].index("@")-1].strip()

        time.sleep(1.0)
        snapshots = simulate_snapshots(game_id, game_date, home_team, away_team)
        all_snapshots.extend(snapshots)

    df = pd.DataFrame(all_snapshots)
    df.to_csv("data/win_prediction_dataset.csv", index=False)
    print(f"[Done] Dataset saved to data/win_prediction_dataset.csv")

if __name__ == "__main__":
    main()
