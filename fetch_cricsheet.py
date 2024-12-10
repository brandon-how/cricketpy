# %%
import requests
import pandas as pd
import numpy as np
import os
import tempfile
import zipfile
from pathlib import Path
from dotenv import load_dotenv
from countries import *
from difflib import get_close_matches
from io import StringIO
from typing import Tuple, Literal, Optional

# Set environment
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)
env = os.getenv("env")

# %%

type = "bbb"
gender = "male"
competition = "tests"

dest_file = f"{competition}_{gender}_csv2.zip"
url = "https://cricsheet.org/downloads/"
subdir = dest_file.replace("_csv2.zip", "") + "_bbb"

# %%
import os
import re
import tempfile
import requests
import zipfile
import pandas as pd
from pathlib import Path


def fetch_cricsheet(type="bbb", gender="male", competition="tests"):
    # Match arguments (similar to match.arg in R)
    valid_types = ["bbb", "match", "player"]
    valid_genders = ["female", "male"]
    if type not in valid_types:
        raise ValueError(f"Invalid type. Expected one of {valid_types}.")
    if gender not in valid_genders:
        raise ValueError(f"Invalid gender. Expected one of {valid_genders}.")

    # Backwards compatibility for competition
    competition_map = {
        "county": "cch",
        "edwards_cup": "cec",
        "heyhoe_flint_trophy": "rhf",
        "multi_day": "mdms",
        "sheffield_shield": "ssh",
        "super_smash": "ssm",
        "the_hundred": "hnd",
        "t20_blast": "ntb",
        "t20is": "t20s",
        "t20is_unofficial": "it20s",
        "wbbl": "wbb",
        "wt20c": "wtc",
    }
    competition = competition_map.get(competition, competition)

    # Construct file names and URL
    destfile = f"{competition}_{gender}_csv2.zip"
    url = f"https://cricsheet.org/downloads/{destfile}"
    subdir = f"{destfile.replace('_csv2.zip', '')}_bbb"
    temp_dir = tempfile.gettempdir()
    destfile_path = os.path.join(temp_dir, destfile)

    # Headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    # Download the file if not already downloaded
    if not os.path.exists(destfile_path):
        response = requests.get(url, headers=headers)
        with open(destfile_path, "wb") as f:
            f.write(response.content)

    # Extract files in zip and get file list
    with zipfile.ZipFile(destfile_path, "r") as z:
        file_list = z.namelist()

    # Categorize files
    file_types = {
        "txt": [f for f in file_list if "txt" in f],
        "info": [f for f in file_list if "_info" in f],
        "allbbb": [f for f in file_list if "all_matches" in f],
        "bbb": [
            f
            for f in file_list
            if "txt" not in f and "_info" not in f and "all_matches" not in f
        ],
    }

    # Determine required files
    if type == "bbb":
        if "all_matches.csv" in file_types["allbbb"]:
            match_files = "all_matches.csv"
        else:
            match_files = file_types["bbb"]
    else:
        match_files = file_types["info"]

    # Extract files into a subdirectory
    output_dir = os.path.join(temp_dir, subdir)
    os.makedirs(output_dir, exist_ok=True)
    with zipfile.ZipFile(destfile_path, "r") as z:
        z.extractall(output_dir, members=match_files)

    # Load and process files
    match_filepaths = [os.path.join(output_dir, f) for f in match_files]

    # Read data from CSVs stored in temp directory
    if type == "bbb":
        dataframes = [pd.read_csv(f).assign(match_id=f) for f in match_filepaths]
        all_matches = pd.concat(dataframes, ignore_index=True)
        all_matches["match_id"] = all_matches["match_id"].str.extract(
            r"(\d+)", expand=False
        )
    else:
        # Need to manually assign colnames to allow pandas to read in data
        colnames = ["info", "key", "value", "player", "hash"]
        all_matches = pd.concat(
            [
                pd.read_csv(f, names=colnames, skiprows=1).assign(match_id=f)
                for f in match_filepaths
            ],
            ignore_index=True,
        )
        all_matches["match_id"] = all_matches["match_id"].str.extract(
            r"(\d+)", expand=False
        )
        if type == "match":
            # Process metadata
            all_matches = all_matches[
                ~all_matches.key.isin(["player", "players", "registry"])
            ]

            # Process team number
            all_matches["team"] = all_matches.key == "team"
            all_matches["team"] = all_matches.groupby("match_id").team.cumsum()
            all_matches["key"] = np.where(
                all_matches["key"] == "team",
                all_matches["key"] + all_matches.team.astype(str),
                all_matches["key"],
            )
            # Process umpire number
            all_matches["umpire"] = all_matches.key == "umpire"
            all_matches["umpire"] = all_matches.groupby("match_id").umpire.cumsum()
            all_matches["key"] = np.where(
                all_matches["key"] == "umpire",
                all_matches["key"] + all_matches.umpire.astype(str),
                all_matches["key"],
            )
            # Keep 2 umpires
            all_matches = all_matches.query("key != 'umpire3'")
            # Deduplicate dates and keep first
            # Residual duplication include match date, TV and reserve umpires, and player of match
            all_matches = all_matches.drop_duplicates(["match_id", "key"], keep="first")
            # Finally create pivot table
            all_matches = all_matches.pivot(
                index="match_id", columns="key", values="value"
            ).reset_index()
            # Reorder columns
            cols_order = [
                "match_id",
                "team1",
                "team2",
                "gender",
                "season",
                "date",
                "event",
                "match_number",
                "venue",
                "city",
                "balls_per_over",
                "toss_winner",
                "toss_decision",
                "player_of_match",
            ]
            cols_order += [col for col in all_matches.columns if col not in cols_order]
            all_matches = all_matches[cols_order]
        else:
            # Process player data
            all_matches = all_matches[all_matches.key.isin(["player", "players"])]
            all_matches = all_matches[["match_id", "value", "player"]]
            all_matches = all_matches.rename(columns={"value": "team"})

    # # Clean data
    # if type == "bbb" and "ball" in all_matches.columns:
    #     t20 = all_matches["ball"].max() <= 21
    #     if t20:
    #         all_matches = cleaning_bbb_t20_cricsheet(
    #             all_matches
    #         )  # Implement cleaning logic separately

    return all_matches.astype(str)


# %%

test = fetch_cricsheet(type="bbb", gender="male", competition="tests")
t20 = fetch_cricsheet(type="bbb", gender="male", competition="t20s")

# %%


def cleaning_bbb_t20_cricsheet(df):
    # Step 1: Add columns for wicket, over number, and extra_ball
    df["wicket"] = ~df["wicket_type"].isin(["", "retired hurt"])
    df["over"] = np.ceil(df["ball"]).astype(int)
    df["extra_ball"] = df["wides"].notna() | df["noballs"].notna()

    # Step 2: Adjust ball values within each over
    df["ball"] = df.groupby(["match_id", "innings", "over"]).cumcount() + 1

    # Step 3: Calculate cumulative runs and wickets
    cumulative = (
        df.groupby(["match_id", "innings"])
        .apply(
            lambda group: pd.DataFrame(
                {
                    "runs_scored_yet": (
                        group["runs_off_bat"] + group["extras"]
                    ).cumsum(),
                    "wickets_lost_yet": group["wicket"].cumsum(),
                    "ball": group["ball"],
                    "over": group["over"],
                }
            )
        )
        .reset_index(drop=True)
    )
    df = df.merge(cumulative, on=["match_id", "innings", "over", "ball"], how="inner")

    # Step 4: Calculate balls in over and balls remaining
    remaining_balls = (
        df.groupby(["match_id", "innings", "over"])
        .apply(
            lambda group: pd.DataFrame(
                {
                    "ball": group["ball"],
                    "extra_ball": group["extra_ball"].cumsum(),
                    "ball_in_over": group["ball"] - group["extra_ball"].cumsum(),
                    "balls_remaining": group.apply(
                        lambda row: (
                            120 - ((row["over"] - 1) * 6 + row["ball_in_over"])
                            if row["innings"] in [1, 2]
                            else 6 - row["ball_in_over"]
                        ),
                        axis=1,
                    ),
                }
            )
        )
        .reset_index(drop=True)
        .drop(columns=["extra_ball"])
    )
    df = df.merge(
        remaining_balls, on=["match_id", "innings", "over", "ball"], how="inner"
    )

    # Step 5: Calculate innings totals
    innings_total = (
        df.groupby(["match_id", "innings"])
        .apply(
            lambda group: pd.Series(
                {"total_score": group["runs_off_bat"].sum() + group["extras"].sum()}
            )
        )
        .reset_index()
    )
    innings_total = innings_total.pivot(
        index="match_id", columns="innings", values="total_score"
    ).reset_index()
    innings_total.columns = ["match_id", "innings1_total", "innings2_total"]

    # Step 6: Merge all data
    df = df.merge(innings_total, on="match_id", how="inner")
    df["target"] = df["innings1_total"] + 1
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date

    # Step 7: Reorder columns
    column_order = [
        "match_id",
        "season",
        "start_date",
        "venue",
        "innings",
        "over",
        "ball",
        "batting_team",
        "bowling_team",
        "striker",
        "non_striker",
        "bowler",
        "runs_off_bat",
        "extras",
        "ball_in_over",
        "extra_ball",
        "balls_remaining",
        "runs_scored_yet",
        "wicket",
        "wickets_lost_yet",
        "innings1_total",
        "innings2_total",
        "target",
        "wides",
        "noballs",
        "byes",
        "legbyes",
        "penalty",
        "wicket_type",
        "player_dismissed",
        "other_wicket_type",
        "other_player_dismissed",
    ]
    df = df[column_order + [col for col in df.columns if col not in column_order]]

    return df
