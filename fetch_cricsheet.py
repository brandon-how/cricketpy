# %%
import requests
import pandas as pd
import numpy as np
import os
import tempfile
import zipfile
from pathlib import Path
from countries import *
from typing import List
from helpers import *

pd.set_option("display.max_columns", 100)


# %%
def process_bbb_data(match_filepaths: List[str]) -> pd.DataFrame:
    """Processes ball-by-ball data from given CSV file paths.

    This function reads a list of CSV file paths containing ball-by-ball data,
    concatenates them into a single DataFrame, and processes the match IDs
    by extracting relevant parts and removing file extensions.

    Args:
        match_filepaths (List[str]): A list of file paths pointing to CSV files
            with ball-by-ball data.

    Returns:
        pd.DataFrame: A DataFrame containing the combined and processed ball-by-ball data.
    """
    dataframes = [pd.read_csv(f).assign(match_id=f) for f in match_filepaths]
    all_matches = pd.concat(dataframes, ignore_index=True)
    all_matches["match_id"] = all_matches["match_id"].str.extract(
        r"([a-zA-Z0-9_\-\.]*$)"
    )
    all_matches["match_id"] = all_matches["match_id"].str.replace(".csv", "")
    return all_matches


# %%
def process_match_metadata(all_matches: pd.DataFrame) -> pd.DataFrame:
    """Processes match metadata to create a structured DataFrame.

    This function processes metadata for cricket matches by handling team
    and umpire details, deduplicating key data points, and pivoting the data
    into a wide-format DataFrame.

    Args:
        all_matches (pd.DataFrame): A DataFrame containing match metadata,
            including columns of `key`, `value`, and `match_id`.

    Returns:
        pd.DataFrame: A wide-format DataFrame with organized metadata for each match.
    """
    # Process metadata
    all_matches = all_matches[~all_matches.key.isin(["player", "players", "registry"])]

    # Process team number
    all_matches["team"] = all_matches["key"] == "team"
    all_matches["team"] = all_matches.groupby("match_id")["team"].cumsum()
    all_matches["key"] = np.where(
        all_matches["key"] == "team",
        all_matches["key"] + all_matches["team"].astype(str),
        all_matches["key"],
    )
    # Process umpire number
    all_matches["umpire"] = all_matches["key"] == "umpire"
    all_matches["umpire"] = all_matches.groupby("match_id")["umpire"].cumsum()
    all_matches["key"] = np.where(
        all_matches["key"] == "umpire",
        all_matches["key"] + all_matches["umpire"].astype(str),
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
    return all_matches


# %%
def process_metadata(match_filepaths: List[str]) -> pd.DataFrame:
    """Processes metadata or player data from given file paths.

    This function reads a list of CSV file paths containing match or player metadata,
    processes the data based on the specified type, and returns a structured DataFrame.

    Args:
        match_filepaths (List[str]): A list of file paths pointing to CSV files
            with metadata or player data.

    Returns:
        pd.DataFrame: A DataFrame containing processed metadata or player data.
    """
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
        r"([a-zA-Z0-9_\-\.]*$)"
    )
    all_matches["match_id"] = all_matches["match_id"].str.replace("_info.csv", "")

    if type == "match":
        all_matches = process_match_metadata(all_matches)
    else:
        # Process player data
        all_matches = all_matches[all_matches.key.isin(["player", "players"])]
        all_matches = all_matches[["match_id", "value", "player"]]
        all_matches = all_matches.rename(columns={"value": "team"})
    return all_matches


# %%
def cleaning_bbb_t20_cricsheet(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and processes ball-by-ball T20 cricket data from Cricsheet.

    This function performs data cleaning and transformation for T20 cricket
    ball-by-ball data. It adjusts ball counts for extras, calculates cumulative
    runs and wickets, handles innings totals, and prepares the dataset with
    derived columns for analysis.

    Args:
        df (pd.DataFrame): A DataFrame containing raw ball-by-ball cricket data
            with columns including `match_id`, `innings`, `ball`, `wicket_type`,
            `runs_off_bat`, and `extras`.

    Returns:
        pd.DataFrame: A cleaned and processed DataFrame with additional columns
            for cumulative metrics, adjusted ball counts, innings totals, and
            target scores, reordered for ease of use.
    """
    df["wicket"] = ~df["wicket_type"].isin(["retired hurt"]) & df["wicket_type"].notna()

    df["over"] = np.ceil(df["ball"]).astype(int)
    df["extra_ball"] = df["wides"].notna() | df["noballs"].notna()

    # Step 2: Adjust ball values within each over
    df["ball"] = df.groupby(["match_id", "innings", "over"]).cumcount() + 1

    # Step 3: Calculate cumulative runs and wickets
    df["runs_scored_yet"] = (
        df.groupby(["match_id", "innings"])["runs_off_bat"].cumsum()
        + df.groupby(["match_id", "innings"])["extras"].cumsum()
    )

    df["wickets_lost_yet"] = df.groupby(["match_id", "innings"])["wicket"].cumsum()

    # Step 4: Calculate balls in over and balls remaining
    df["extra_ball"] = df.groupby(["match_id", "innings", "over"])[
        "extra_ball"
    ].cumsum()
    df["ball_in_over"] = df["ball"] - df["extra_ball"]
    df["balls_remaining"] = np.where(
        df["innings"].isin([1, 2]),
        120 - ((df["over"] - 1) * 6 + df["ball_in_over"]),
        6 - df["ball_in_over"],
    )  # For tie breaker, each team plays 1 over (6 balls, innings 3 & 4)

    # Step 5: Calculate innings totals
    innings_total = (
        df.groupby(["match_id", "innings"])["runs_off_bat"].sum()
        + df.groupby(["match_id", "innings"])["extras"].sum()
    ).reset_index(name="total_score")

    innings_total = (
        innings_total.pivot(index="match_id", columns="innings", values="total_score")
        .rename(columns={1: "innings1_total", 2: "innings2_total"})
        .filter(["innings1_total", "innings2_total"])
        .reset_index()
    )

    # Step 6: Merge all data
    df = df.merge(innings_total, on="match_id", how="inner")
    df["target"] = df["innings1_total"] + 1

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


# %%
def fetch_cricsheet(type="bbb", gender="male", competition="tests"):
    """
    Fetches and processes cricket match data from Cricsheet based on the provided parameters.

    This function downloads a zipped CSV file from the Cricsheet website, extracts it, processes the data, and returns
    a cleaned pandas DataFrame with the relevant match information.

    Args:
        type (str): The type of data to fetch. One of "bbb" (default), "match", or "player".
        gender (str): The gender category of the data. Must be one of "male" or "female".
        competition (str): The competition for which to fetch the data. This can be one of a number of cricket competitions
                           such as "tests", "county", "the_hundred", etc. See `competition_map` for full list.

    Returns:
        pandas.DataFrame: A DataFrame containing the processed cricket match data.

    Raises:
        ValueError: If an invalid value is provided for `type` or `gender`.
        requests.exceptions.RequestException: If there is an issue with the HTTP request during download.
        zipfile.BadZipFile: If the downloaded file is not a valid zip file.
        KeyError: If the expected CSV file is not found within the zip archive.

    Examples:
        >>> df = fetch_cricsheet(type="bbb", gender="male", competition="tests")
        >>> print(df.head())
        ...

    Notes:
        - The function handles backwards compatibility for the `competition` parameter using a `competition_map`.
        - If the `type` is "bbb" and the competition contains ball-by-ball data, the function will clean the data specifically for T20 matches.
        - The function utilizes temporary directories for downloading and extracting files.
        - The cleaned data is returned as a pandas DataFrame after processing and cleaning.

    """
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

    # Temporary directory
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
        response.raise_for_status()
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
        all_matches = process_bbb_data(match_filepaths)
    else:
        all_matches = process_metadata(match_filepaths)

    # Clean data
    if type == "bbb" and "ball" in all_matches.columns:
        if all_matches["ball"].max() <= 21:
            all_matches = cleaning_bbb_t20_cricsheet(all_matches)

    all_matches = dtype_clean(all_matches)

    return all_matches


# %%

# test = fetch_cricsheet(type="bbb", gender="male", competition="tests")
t20 = fetch_cricsheet(type="bbb", gender="male", competition="t20s")

# %%
df = t20.copy()

# %%
