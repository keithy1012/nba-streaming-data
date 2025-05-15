import xgboost as xgb
import numpy as np
model = xgb.XGBClassifier()
model.load_model("model/win_predictor.json")

'''
"feature_names":["home_team_wins","home_team_losses",
"away_team_wins","away_team_losses","seconds_left",
"period","home_score","away_score","score_diff","home_fg_pct",
"home_3pt_pct","home_ft_pct","home_to","home_reb",
"away_fg_pct","away_3pt_pct","away_ft_pct","away_to","away_reb",
"score_diff_momentum","points_scored_last_2min_home",
"points_scored_last_2min_away"]
'''
new_data = np.array([73, 9, 9, 73, 2, 4, 100, 80, 20, .4, .3, .9, 2, 20, .34, .23, .78, 5, 14, 10, 4, 5]).reshape(1, -1)
preds = model.predict(new_data)
print(preds)