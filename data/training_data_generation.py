from nba_api.stats.endpoints import playbyplayv2, boxscoretraditionalv2, teamgamelog, leaguegamefinder
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.static import teams
import pandas as pd
from tqdm import tqdm
import time

def get_past_games(season="2023-24", num_games=50):
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    games = gamefinder.get_data_frames()[0]
    return games.head(num_games)

def simulate_snapshots(game_id, interval=120):
    try:
        pbp = playbyplayv2.PlayByPlayV2(game_id=game_id).get_data_frames()[0]
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id).get_data_frames()[1]  # [1] = team stats

        game_date = pd.to_datetime(pbp.iloc[0]['GAME_DATE'])

        # Get logs for both teams
        home_team_id = home_team['TEAM_ID']
        away_team_id = away_team['TEAM_ID']

        home_log = teamgamelog.TeamGameLog(team_id=home_team_id, season='2023').get_data_frames()[0]
        away_log = teamgamelog.TeamGameLog(team_id=away_team_id, season='2023').get_data_frames()[0]

        # Filter games before this one
        home_before = home_log[pd.to_datetime(home_log['GAME_DATE']) < game_date]
        away_before = away_log[pd.to_datetime(away_log['GAME_DATE']) < game_date]

        home_wins = (home_before['WL'] == 'W').sum()
        home_losses = (home_before['WL'] == 'L').sum()
        away_wins = (away_before['WL'] == 'W').sum()
        away_losses = (away_before['WL'] == 'L').sum()


        players = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id).get_data_frames()[0]
        # Sort by MIN or PTS
        home_players = players[players['TEAM_ID'] == home_team['TEAM_ID']].sort_values('MIN', ascending=False)
        away_players = players[players['TEAM_ID'] == away_team['TEAM_ID']].sort_values('MIN', ascending=False)

        # Get top 1 player stats (or top 3 if you want more)
        home_top = home_players.iloc[0]
        away_top = away_players.iloc[0]


        final_row = pbp[pbp['EVENTMSGTYPE'] == 13]
        winner = pbp['SCOREMARGIN'].dropna().iloc[-1]
        label = 1 if int(winner) > 0 else 0  # Home team wins

        # Identify home/away
        home_team = box.loc[box['TEAM_CITY'].duplicated(keep='last') == False].iloc[0]
        away_team = box.loc[box['TEAM_CITY'].duplicated(keep='first') == False].iloc[0]

        snapshots = []
        current_time = 2880  # 48 * 60
        for idx, row in pbp.iterrows():
            period = row['PERIOD']
            minutes = row['PCTIMESTRING'].split(':')
            seconds_left = int(minutes[0]) * 60 + int(minutes[1])
            time_left = (4 - period) * 720 + seconds_left

            if time_left <= current_time - interval:
                score = row['SCORE']
                if isinstance(score, str) and '-' in score:
                    home_score, away_score = map(int, score.split('-'))
                    # Find score 2 minutes ago
                    target_time = time_left + 120
                    previous_score = None
                    for j in range(idx, len(pbp)):
                        future_row = pbp.iloc[j]
                        pctimestr = future_row['PCTIMESTRING']
                        if isinstance(pctimestr, str):
                            m, s = map(int, pctimestr.split(":"))
                            future_seconds_left = (4 - future_row['PERIOD']) * 720 + m * 60 + s
                            if future_seconds_left <= target_time:
                                score = future_row['SCORE']
                                if isinstance(score, str) and '-' in score:
                                    home_prev, away_prev = map(int, score.split('-'))
                                    previous_score = (home_prev, away_prev)
                                    break

                    if previous_score:
                        home_momentum = home_score - previous_score[0]
                        away_momentum = away_score - previous_score[1]
                        score_diff_momentum = (home_score - away_score) - (previous_score[0] - previous_score[1])
                    else:
                        home_momentum = away_momentum = score_diff_momentum = 0


                    # Snapshot with team stats
                    snapshot = {
                        'game_id': game_id,
                        'home_team_wins': home_wins,
                        'home_team_losses': home_losses,
                        'away_team_wins': away_wins,
                        'away_team_losses': away_losses,
                        'seconds_left': time_left,
                        'period': period,
                        'home_score': home_score,
                        'away_score': away_score,
                        'score_diff': home_score - away_score,
                        'home_fg_pct': float(home_team['FG_PCT']),
                        'home_3pt_pct': float(home_team['FG3_PCT']),
                        'home_ft_pct': float(home_team['FT_PCT']),
                        'home_ast': int(home_team['AST']),
                        'home_to': int(home_team['TO']),
                        'home_reb': int(home_team['REB']),
                        'away_fg_pct': float(away_team['FG_PCT']),
                        'away_3pt_pct': float(away_team['FG3_PCT']),
                        'away_ft_pct': float(away_team['FT_PCT']),
                        'away_ast': int(away_team['AST']),
                        'away_to': int(away_team['TO']),
                        'away_reb': int(away_team['REB']),
                        'score_diff_momentum': score_diff_momentum,
                        'points_scored_last_2min_home': home_momentum,
                        'points_scored_last_2min_away': away_momentum,
                        'home_top1_pts': int(home_top['PTS']),
                        'home_top1_ast': int(home_top['AST']),
                        'home_top1_reb': int(home_top['REB']),
                        'away_top1_pts': int(away_top['PTS']),
                        'away_top1_ast': int(away_top['AST']),
                        'away_top1_reb': int(away_top['REB']),
                        'label': label
                    }
                    snapshots.append(snapshot)
                    current_time = time_left

        return snapshots

    except Exception as e:
        print(f"[Error] Game {game_id}: {e}")
        return []

def main():
    all_snapshots = []
    games = get_past_games()
    for _, game in tqdm(games.iterrows(), total=len(games)):
        game_id = game['GAME_ID']
        time.sleep(0.6)
        snapshots = simulate_snapshots(game_id)
        all_snapshots.extend(snapshots)

    df = pd.DataFrame(all_snapshots)
    df.to_csv("data/win_prediction_dataset.csv", index=False)
    print(f"[Done] Dataset saved to data/win_prediction_dataset.csv")

if __name__ == "__main__":
    main()
