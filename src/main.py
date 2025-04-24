#!/usr/bin/env python

import polars as pl
from src.projection import batter, pitcher, mops

filepath = "/mnt/c/Users/lucio/OneDrive/Documents/Out of the Park Developments/OOTP Baseball 26/saved_games/Yankees.lg/import_export/csv"
ID = 810

players = (
    pl.scan_csv(filepath + "/players.csv")
    .filter(pl.col("retired") != 1)
    .sort("player_id")
)

# result = pl.read_csv(filepath + "/players_scouted_ratings.csv")
# result = result.filter(pl.col("scouting_coach_id") == ID)
# result = result.select(pl.col("scouting_coach_id"))

# print(result)

scouted_ratings = (
    pl.scan_csv(filepath + "/players_scouted_ratings.csv")
    .filter(pl.col("scouting_coach_id") == ID)
    .sort("player_id")
)

df = players.join(scouted_ratings, on="player_id")

df = df.rename(
    {
        "team_id": "team_id_x",
        "league_id": "league_id_x",
        "position": "position_x",
        "role": "role_x",
    }
)

batters = batter.Batters(filepath)

merged_df = (
    df.join(batters.calculate_rates(), on="player_id", how="left")
    .join(
        batters.calculate_yearly_sum().select(["player_id", "pa", "war"]),
        on="player_id",
        how="left",
    )
    .rename({"war": "WAR_actual"})
    .with_columns((650 / pl.col("pa") * pl.col("WAR_actual")).alias("sWAR_actual"))
)

pitchers = pitcher.Pitchers(filepath)

pitchers_yearly_sum = pitchers.calculate_yearly_sum().select(
    ["player_id", "ip", "war", "ra9war"]
)

sWAR_calc = (100 / pl.col("ip")) * pl.col("WAR_actual_p")
merged_df = (
    merged_df.join(pitchers_yearly_sum, on="player_id", how="left")
    .rename({"war": "WAR_actual_p"})
    .with_columns(sWAR_calc.alias("sWAR_actual_p"))
    .with_columns(pl.col("ip").fill_nan(None))
)

columns_to_drop = [
    "nick_name",
    "city_of_birth_id",
    "nation_id",
    "second_nation_id",
    "last_league_id",
    "last_team_id",
    "last_organization_id",
    "language_ids0",
    "language_ids1",
    "uniform_number",
    "experience",
    "person_type",
    "historical_id",
    "historical_team_id",
    "best_contract_offer_id",
    "injury_is_injured",
    "injury_dtd_injury",
    "injury_career_ending",
    "injury_dl_left",
    "injury_dl_playoff_round",
    "injury_left",
    "dtd_injury_effect",
    "dtd_injury_effect_hit",
    "dtd_injury_effect_throw",
    "dtd_injury_effect_run",
    "injury_id",
    "injury_id2",
    "injury_dtd_injury2",
    "injury_left2",
    "dtd_injury_effect2",
    "dtd_injury_effect_hit2",
    "dtd_injury_effect_throw2",
    "dtd_injury_effect_run2",
    "prone_overall",
    "prone_leg",
    "prone_back",
    "prone_arm",
    "fatigue_pitches0",
    "fatigue_pitches1",
    "fatigue_pitches2",
    "fatigue_pitches3",
    "fatigue_pitches4",
    "fatigue_pitches5",
    "fatigue_points",
    "fatigue_played_today",
    "running_ratings_speed_x",
    "running_ratings_stealing_x",
    "running_ratings_baserunning_x",
    "college",
    "school",
    "commit_school",
    "hidden",
    "turned_coach",
    "hall_of_fame",
    "rust",
    "inducted",
    "strategy_override_team",
    "strategy_stealing",
    "strategy_running",
    "strategy_bunt_for_hit",
    "strategy_sac_bunt",
    "strategy_hit_run",
    "strategy_hook_start",
    "strategy_hook_relief",
    "strategy_pitch_count",
    "strategy_pitch_around",
    "strategy_never_pinch_hit",
    "strategy_defensive_sub",
    "strategy_dtd_sit_min",
    "strategy_dtd_allow_ph",
    "local_pop",
    "national_pop",
    "draft_protected",
    "morale",
    "morale_player_performance",
    "morale_team_performance",
    "morale_team_transactions",
    "expectation",
    "morale_player_role",
    "on_loan",
    "loan_league_id",
    "loan_team_id",
    "team_id_y",
    "league_id_y",
    "position_y",
    "role_y",
    "acquired",
    "acquired_date",
    "draft_year",
    "draft_round",
    "draft_supplemental",
    "draft_pick",
    "draft_overall_pick",
    "draft_eligible",
    "hsc_status",
    "redshirt",
    "picked_in_draft",
    "draft_league_id",
    "draft_team_id",
    "morale_mod",
    "morale_team_chemistry",
    "scouting_coach_id",
    "scouting_team_id",
]

merged_df = merged_df.drop(columns_to_drop, strict=False)

# create new columns to preseve the 20-80 scale ratings exported by OOTP 26 (actually this is on a 20-100 scale as super-ratings of 85, 90, 95 and 100 are possible)
# Mapping of original column names to new names
column_mapping = {
    "batting_ratings_overall_eye": "eye2080",
    "batting_ratings_overall_strikeouts": "avK2080",
    "batting_ratings_overall_power": "pow2080",
    "batting_ratings_overall_gap": "gap2080",
    "batting_ratings_overall_babip": "babip2080",
    "batting_ratings_talent_eye": "eye2080p",
    "batting_ratings_talent_strikeouts": "avK2080p",
    "batting_ratings_talent_power": "pow2080p",
    "batting_ratings_talent_gap": "gap2080p",
    "batting_ratings_talent_babip": "babip2080p",
    "fielding_ratings_catcher_ability": "cabi2080",
    "fielding_ratings_catcher_arm": "carm2080",
    "fielding_ratings_infield_range": "ifrng2080",
    "fielding_ratings_infield_error": "iferr2080",
    "fielding_ratings_infield_arm": "ifarm2080",
    "fielding_ratings_turn_doubleplay": "turndp2080",
    "fielding_ratings_outfield_arm": "ofarm2080",
    "fielding_ratings_outfield_range": "ofrng2080",
    "fielding_ratings_outfield_error": "oferr2080",
    "pitching_ratings_pitches_fastball": "fb2080",
    "pitching_ratings_pitches_slider": "sl2080",
    "pitching_ratings_pitches_curveball": "crv2080",
    "pitching_ratings_pitches_screwball": "scrw2080",
    "pitching_ratings_pitches_forkball": "frk2080",
    "pitching_ratings_pitches_changeup": "chng2080",
    "pitching_ratings_pitches_sinker": "sink2080",
    "pitching_ratings_pitches_splitter": "spli2080",
    "pitching_ratings_pitches_knuckleball": "knuc2080",
    "pitching_ratings_pitches_cutter": "cut2080",
    "pitching_ratings_pitches_circlechange": "cchng2080",
    "pitching_ratings_pitches_knucklecurve": "kcurv2080",
    "pitching_ratings_misc_stamina": "stam2080",
    "pitching_ratings_overall_stuff": "stuff2080",
    "pitching_ratings_overall_control": "ctrl2080",
    "pitching_ratings_overall_movement": "mvt2080",
    "pitching_ratings_overall_hra": "hra2080",
    "pitching_ratings_overall_pbabip": "pbabip2080",
    "pitching_ratings_pitches_talent_fastball": "fb2080p",
    "pitching_ratings_pitches_talent_slider": "sl2080p",
    "pitching_ratings_pitches_talent_curveball": "crv2080p",
    "pitching_ratings_pitches_talent_screwball": "scrw2080p",
    "pitching_ratings_pitches_talent_forkball": "frk2080p",
    "pitching_ratings_pitches_talent_changeup": "chng2080p",
    "pitching_ratings_pitches_talent_sinker": "sink2080p",
    "pitching_ratings_pitches_talent_splitter": "spli2080p",
    "pitching_ratings_pitches_talent_knuckleball": "knuc2080p",
    "pitching_ratings_pitches_talent_cutter": "cut2080p",
    "pitching_ratings_pitches_talent_circlechange": "cchng2080p",
    "pitching_ratings_pitches_talent_knucklecurve": "kcurv2080p",
    "pitching_ratings_talent_stuff": "stuff2080p",
    "pitching_ratings_talent_control": "ctrl2080p",
    "pitching_ratings_talent_movement": "mvt2080p",
    "pitching_ratings_talent_hra": "hra2080p",
    "pitching_ratings_talent_pbabip": "pbabip2080p",
}

merged_df = merged_df.with_columns(
    [pl.col(orig).alias(copied) for orig, copied in column_mapping.items()]
)


# replace the 20-100 ratings with ratings on a 1-250 scale in line with the export from OOTP 2024, which the calculations below are based on
# Define the find-replace mapping between the 20-100 scale and the 1-250 scale
replace_map = {
    20: 6,
    25: 20,
    30: 35,
    35: 52,
    40: 69,
    45: 85,
    50: 101,
    55: 117,
    60: 134,
    65: 150,
    70: 166,
    75: 181,
    80: 201,
    85: 213,
    90: 225,
    95: 238,
    100: 250,
}

# List of columns to apply the replacement
columns_to_replace = [
    "batting_ratings_overall_eye",
    "batting_ratings_overall_strikeouts",
    "batting_ratings_overall_power",
    "batting_ratings_overall_gap",
    "batting_ratings_overall_babip",
    "batting_ratings_talent_eye",
    "batting_ratings_talent_strikeouts",
    "batting_ratings_talent_power",
    "batting_ratings_talent_gap",
    "batting_ratings_talent_babip",
    "fielding_ratings_catcher_ability",
    "fielding_ratings_catcher_arm",
    "fielding_ratings_catcher_framing",
    "fielding_ratings_infield_range",
    "fielding_ratings_infield_error",
    "fielding_ratings_infield_arm",
    "fielding_ratings_turn_doubleplay",
    "fielding_ratings_outfield_arm",
    "fielding_ratings_outfield_range",
    "fielding_ratings_outfield_error",
    "pitching_ratings_pitches_fastball",
    "pitching_ratings_pitches_slider",
    "pitching_ratings_pitches_curveball",
    "pitching_ratings_pitches_screwball",
    "pitching_ratings_pitches_forkball",
    "pitching_ratings_pitches_changeup",
    "pitching_ratings_pitches_sinker",
    "pitching_ratings_pitches_splitter",
    "pitching_ratings_pitches_knuckleball",
    "pitching_ratings_pitches_cutter",
    "pitching_ratings_pitches_circlechange",
    "pitching_ratings_pitches_knucklecurve",
    "pitching_ratings_misc_stamina",
    "pitching_ratings_overall_stuff",
    "pitching_ratings_overall_control",
    "pitching_ratings_overall_movement",
    "pitching_ratings_pitches_talent_fastball",
    "pitching_ratings_pitches_talent_slider",
    "pitching_ratings_pitches_talent_curveball",
    "pitching_ratings_pitches_talent_screwball",
    "pitching_ratings_pitches_talent_forkball",
    "pitching_ratings_pitches_talent_changeup",
    "pitching_ratings_pitches_talent_sinker",
    "pitching_ratings_pitches_talent_splitter",
    "pitching_ratings_pitches_talent_knuckleball",
    "pitching_ratings_pitches_talent_cutter",
    "pitching_ratings_pitches_talent_circlechange",
    "pitching_ratings_pitches_talent_knucklecurve",
    "pitching_ratings_talent_stuff",
    "pitching_ratings_talent_control",
    "pitching_ratings_talent_movement",
]

merged_df = merged_df.with_columns(pl.col(columns_to_replace).replace(replace_map))

war_df = merged_df.mops.with_toWAR()

print(war_df.lazy().collect().head(n=10))

# html = final._repr_html_()
# # Add some JavaScript for sortable and filterable table
# html = (
#     """
# <!DOCTYPE html>
# <html>
# <head>
# <script src="https://code.jquery.com/jquery-3.7.1.js"></script>
# <link
#   rel="stylesheet"
#   href="https://cdn.datatables.net/2.2.2/css/dataTables.dataTables.css"
# />
# <!-- DataTables JS -->
# <script
#   src="https://cdn.datatables.net/2.2.2/js/dataTables.js"
# ></script>
# </head>
# <body>
# %s
# <script>
# $(document).ready(function(){
#     $('table').addClass("display")
#     $('table').removeClass("dataframe")
#     $('table').css("border", 'none')
#
#     $('table').DataTable({
#         "paging": true,
#         "pageLength": 50
#     });
# });
# </script>
# </body>
# </html>
# """
#     % html
# )
#
# with open("df.html", "w") as f:
#     f.write(html)
