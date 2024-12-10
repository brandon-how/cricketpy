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
    if type == "bbb":
        dataframes = [pd.read_csv(f) for f in match_filepaths]
        all_matches = pd.concat(dataframes, ignore_index=True)
    else:
        all_matches = pd.concat(
            [
                pd.read_csv(f, names=["col_to_delete", "key", "value"], skiprows=1)
                for f in match_filepaths
            ],
            ignore_index=True,
        )
        all_matches["match_id"] = all_matches["path"].str.extract(
            r"([a-zA-Z0-9_\-\.]*_info.csv)"
        )[0]
        if type == "match":
            # Process metadata
            all_matches = all_matches[
                ~all_matches["key"].isin(["player", "players", "registry"])
            ]
            all_matches = all_matches.pivot(
                index="match_id", columns="key", values="value"
            )
        else:
            # Process player data
            all_matches = all_matches[all_matches["key"].isin(["player", "players"])]

    # Clean data
    if type == "bbb" and "ball" in all_matches.columns:
        t20 = all_matches["ball"].max() <= 21
        if t20:
            all_matches = cleaning_bbb_t20_cricsheet(
                all_matches
            )  # Implement cleaning logic separately

    return all_matches
