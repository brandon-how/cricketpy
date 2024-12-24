# %%
import requests
import pandas as pd
import numpy as np
from countries import *
from difflib import get_close_matches
from io import StringIO
from typing import Literal, Optional
from helpers import *

# %%


def participation_status(df: pd.DataFrame, check_col: str) -> np.ndarray:
    """
    Determine the participation status of players based on values in a specified DataFrame column.

    The function checks for specific keywords in the `check_col` column of the input DataFrame and assigns a
    participation status based on the following conditions:
      - "absent" if the string contains "absent".
      - "dnb" if the string contains "dnb".
      - "tdnb" if the string contains "tdnb".
      - "sub" if the string contains "sub".
      - "b" if none of the above conditions are met.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be processed.
        check_col (str): The name of the column in the DataFrame to check for participation status.

    Returns:
        np.ndarray: An array of strings representing the participation status for each row in the input DataFrame.

    """
    participation = np.where(
        df[check_col].str.contains("absent", case=False, na=False),
        "absent",
        np.where(
            df[check_col].str.contains("dnb", case=False, na=False),
            "dnb",
            np.where(
                df[check_col].str.contains("tdnb", case=False, na=False),
                "tdnb",
                np.where(
                    df[check_col].str.contains("sub", case=False, na=False),
                    "sub",
                    "b",
                ),
            ),
        ),
    )

    return participation


# %%


def clean_batting_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and processes batting data in a pandas DataFrame for ESPNcricinfo batting statistics.

    This function performs a series of cleaning and transformation steps on a batting statistics DataFrame.
    It standardizes column names, extracts additional data, computes new metrics, and ensures proper data types.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing raw batting data. Expected column names include abbreviations or
        raw formats such as "Mat", "Inns", "NO", "HS", "Ave", "BF", etc.

    Returns
    -------
    pd.DataFrame
        A cleaned and processed batting DataFrame with standardized column names and data formats.

    Notes
    -----
    - The function handles both "career view" and "innings view" based on the presence of the "matches" column.
    - Columns are renamed to more descriptive names (e.g., "Mat" to "matches", "HS" to "highscore").
    - High scores with a `*` (not-out) indicator are handled, and a new column `highscore_notout` is created.
    - Batting averages are calculated as `runs / (innings - not_outs)` in career view.
    - Participation status (e.g., "absent", "dnb", "tdnb", "sub") is determined from the "runs" column.
    - Country information is extracted from the "player" column if present.

    """
    # Cast to string then replace
    df = df.replace("-", np.nan)
    # Drop and/or rename columns
    df.columns = df.columns.str.lower()
    if "unnamed: 8" in df.columns:
        df = df.drop(columns="unnamed: 8")
    df = df.rename(
        columns={
            "mat": "matches",
            "inns": "innings",
            "no": "not_outs",
            "hs": "highscore",
            "ave": "average",
            "100": "hundreds",
            "50": "fifties",
            "0": "ducks",
            "sr": "strike_rate",
            "bf": "balls_faced",
            "4s": "fours",
            "6s": "sixes",
            "mins": "minutes",
            "start date": "date",
        }
    )

    # Career view
    if "matches" in df.columns:
        # Add columns
        df["highscore_notout"] = df["highscore"].str.contains("\\*", na=False)
        if "span" in df.columns:
            df["start"] = df["span"].str.split("-").str[0]
            df["end"] = df["span"].str.split("-").str[1]
        # Clean column
        df["highscore"] = df["highscore"].str.replace("*", "")
        # Transform dtypes
        df = dtype_clean(df)
        # Batting average
        df["average"] = df["runs"] / (df["innings"] - df["not_outs"])
    # Innings view
    else:
        # Add columns
        df["not_out"] = df["runs"].str.contains("\\*", na=False)
        # Clean columns
        df["opposition"] = df["opposition"].str.replace(
            "v | Women| Wmn", "", regex=True
        )
        df["opposition"] = df["opposition"].apply(rename_country)
        df["runs"] = df["runs"].str.replace("*", "")
        # Participation status - init to b
        df["participation"] = participation_status(df, "runs")
        # Clean dtypes
        df = dtype_clean(df)

    # Further cleaning
    if "balls_faced" in df.columns:
        df["runs_numeric"] = np.where(
            df["runs"].str.isnumeric().astype(bool).fillna(False), df["runs"], 0
        )
        df["strike_rate"] = df["runs_numeric"].astype(float) / df["balls_faced"] * 100

    # Extract country
    if df["player"].str.contains(r"\(", regex=True).any():
        df["country"] = df["player"].str.extract(r"\(([a-zA-Z /\-]+)\)")
        df["country"] = df["country"].str.replace(r"-W", "", regex=True)
        df["country"] = df["country"].apply(rename_country)
        df["player"] = (
            df["player"].str.replace(r"\([a-zA-Z /\-]+\)", "", regex=True).str.strip()
        )

    # Reorder vars
    if "matches" in df.columns:
        cols_order = [
            "player",
            "country",
            "start",
            "end",
            "matches",
            "innings",
            "not_outs",
            "runs",
            "highscore",
            "highscore_notout",
            "average",
            "balls_faced",
            "strike_rate",
            "hundreds",
            "fifties",
            "ducks",
            "fours",
            "sixes",
        ]
    else:
        cols_order = [
            "date",
            "player",
            "country",
            "runs",
            "not_out",
            "minutes",
            "balls_faced",
            "fours",
            "sixes",
            "strike_rate",
            "innings",
            "participation",
            "opposition",
            "ground",
        ]
    cols_order = [col for col in cols_order if col in df.columns]
    df = df[cols_order]
    return df


# %%


def clean_bowling_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and processes bowling data in a pandas DataFrame for ESPNcricinfo statistics.

    This function performs a series of cleaning, transformation, and computation steps on a bowling statistics DataFrame.
    It standardizes column names, extracts additional information, computes new metrics, and ensures proper data formats.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing raw bowling data. Expected column names include abbreviations or
        raw formats such as "Mat", "Inns", "Mdns", "Wkts", "BBI", "Ave", "Econ", "SR", etc.

    Returns
    -------
    pd.DataFrame
        A cleaned and processed bowling DataFrame with standardized column names, computed metrics, and consistent data formats.

    Notes
    -----
    - The function handles both "career view" and "innings view" based on the presence of the "matches" column.
    - Columns are renamed to more descriptive names (e.g., "Mat" to "matches", "Mdns" to "maidens", "BBI" to "best_bowling_innings").
    - Participation status (e.g., "absent", "dnb", "tdnb", "sub") is determined based on the "overs" column.
    - Bowling averages are calculated as `runs / wickets`, and economy rates are recomputed to avoid rounding issues.
    - Country information is extracted from the "player" column if present.
    - The "span" column, if available, is split into "start" and "end" years.
    - Approximate balls are calculated from the "overs" column as `(trunc(overs) * 6 + (overs % 1) * 10)`.

    """

    # Cast to string then replace
    df = df.replace("-", np.nan)

    # Drop and/or rename columns
    df.columns = df.columns.str.lower()
    if "unnamed: 8" in df.columns:
        df = df.drop(columns="unnamed: 8")
    df = df.rename(
        columns={
            "mat": "matches",
            "inns": "innings",
            "mdns": "maidens",
            "wkts": "wickets",
            "bbi": "best_bowling_innings",
            "bbm": "best_bowling_match",
            "ave": "average",
            "econ": "economy",
            "sr": "strike_rate",
            "4": "four_wickets",
            "5": "five_wickets",
            "10": "ten_wickets",
            "start date": "date",
        }
    )

    # Innings view
    if "matches" not in df.columns:
        df["opposition"] = df["opposition"].str.replace(
            "v | Women| Wmn", "", regex=True
        )
        df["opposition"] = df["opposition"].apply(rename_country)

    # Add depending on columns
    if "span" in df.columns:
        df["start"] = df["span"].str.split("-").str[0]
        df["end"] = df["span"].str.split("-").str[1]

    # Participation status - init to b
    if "overs" in df.columns:
        df["participation"] = participation_status(df, "overs")
        # Impute non-balling overs to 0
        df["overs"] = np.where(df["participation"] != "b", np.nan, df["overs"])

    # Clean dtypes
    df = dtype_clean(df)

    # Calculate average and avoid rounding issues
    df["average"] = df["runs"] / df["wickets"]

    # Calculate (approximate) balls
    if "balls" not in df.columns:
        df["balls"] = np.trunc(df["overs"]) * 6 + (df["overs"] % 1) * 10

    # Recompute economy to avoid rounding issues
    # Do not recompute if difference is too large
    threshold = 0.05
    economy = df["runs"] / (df["balls"] / 6)
    different = np.abs(round(economy, 2) - df["economy"]) > threshold
    if "economy" in df.columns:
        df["economy"] = np.where(different, df["economy"], economy)
    else:
        df["economy"] = economy

    # (Re)Compute strike rate
    df["strike_rate"] = df["balls"] / df["wickets"]

    # Extract country
    if df["player"].str.contains(r"\(", regex=True).any():
        df["country"] = df["player"].str.extract(r"\(([a-zA-Z /\-]+)\)")
        df["country"] = df["country"].str.replace(r"-W", "", regex=True)
        df["country"] = df["country"].apply(rename_country)
        df["player"] = (
            df["player"].str.replace(r"\([a-zA-Z /\-]+\)", "", regex=True).str.strip()
        )

    # Reorder vars
    if "matches" in df.columns:
        cols_order = [
            "player",
            "country",
            "start",
            "end",
            "matches",
            "innings",
            "overs",
            "balls",
            "maidens",
            "runs",
            "wickets",
            "average",
            "economy",
            "strike_rate",
            "best_bowling_innings",
            "best_bowling_match",
            "four_wickets",
            "five_wickets",
            "ten_wickets",
        ]
    else:
        cols_order = [
            "date",
            "player",
            "country",
            "overs",
            "balls",
            "maidens",
            "runs",
            "wickets",
            "average",
            "economy",
            "strike_rate",
            "innings",
            "participation",
            "opposition",
            "ground",
        ]
    cols_order = [col for col in cols_order if col in df.columns]
    df = df[cols_order]

    return df


# %%
def fetch_cricinfo(
    matchtype: Literal["test", "odi", "t20"],
    sex: Literal["men", "women"],
    activity: Literal["batting", "bowling", "fielding"],
    view: Literal["innings", "career"],
    country: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch cricket statistics from ESPNcricinfo based on the specified parameters.

    This function scrapes cricket data for the specified match type, sex, activity,
    and view. Optionally, data can be filtered for a specific country. The data
    is paginated, and all pages are combined into a single Pandas DataFrame.

    Args:
        matchtype (Literal["test", "odi", "t20"]):
            The format of the cricket match to fetch data for.
            Options are "test", "odi", or "t20".
        sex (Literal["men", "women"]):
            Specifies whether the data is for men's or women's cricket.
        activity (Literal["batting", "bowling", "fielding"]):
            The type of cricket activity to fetch data for.
            Options are "batting", "bowling", or "fielding".
        view (Literal["innings", "career"]):
            The perspective of the data: "innings" for individual innings data
            or "career" for aggregated career data.
        country (Optional[str], default=None):
            The country to filter data for. If not provided, data for all teams
            is returned.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the fetched cricket data.

    Raises:
        ValueError: If any of the input parameters are invalid or if the country
            is not found in the predefined list.
        HTTPError: If an HTTP error occurs while fetching data.
        RuntimeError: If the HTML structure of the webpage is unexpected, if parsing
            fails, or if no data is available.

    Examples:
        Fetch batting data for men's Test matches for England:

        >>> fetch_cricinfo(
        ...     matchtype="test",
        ...     sex="men",
        ...     activity="batting",
        ...     view="career",
        ...     country="England"
        ... )

        Fetch bowling data for women's T20 matches across all countries:

        >>> fetch_cricinfo(
        ...     matchtype="t20",
        ...     sex="women",
        ...     activity="bowling",
        ...     view="innings"
        ... )

    Notes:
        - The function relies on ESPNcricinfo's stats page and may break if
          the page structure changes.
        - Pagination is handled automatically, with a maximum limit of 100 pages.
        - The `country` argument uses fuzzy matching to identify the best match
          for the provided country name.
    """
    # Check inputs
    if matchtype not in ["test", "odi", "t20"]:
        raise ValueError("Invalid matchtype. Must be 'test', 'odi', or 't20'.")
    if sex not in ["men", "women"]:
        raise ValueError("Invalid sex. Must be 'men' or 'women'.")
    if activity not in ["batting", "bowling", "fielding"]:
        raise ValueError(
            "Invalid activity. Must be 'batting', 'bowling', or 'fielding'."
        )
    if view not in ["innings", "career"]:
        raise ValueError("Invalid view. Must be 'innings' or 'career'.")

    # Transform everything to lower case
    matchtype = matchtype.lower()
    sex = sex.lower()
    activity = activity.lower()
    view = view.lower()

    # View text and max pagination
    if view == "innings":
        view_text = ";view=innings"
        max_pages = 3000
    else:
        view_text = ""
        max_pages = 100

    # URL signifier for match type
    matchclass = ["test", "odi", "t20"].index(matchtype) + 1 + 7 * (sex == "women")

    # Team number matching for input country
    if country is not None:
        country_sex = men if sex == "men" else women
        # Get closest country match
        country_match = get_close_matches(
            country.lower(), map(str.lower, country_sex.keys()), n=1, cutoff=0.5
        )
        if (
            len(country_match) == 0
            or country_match[0].title() not in country_sex.keys()
        ):
            raise ValueError("Country not found")
        # Get team code and team URL segment
        team = country_sex[country_match[0].title()]
        team_text = f";team={team}"
    else:
        team_text = ""

    # Define base URL
    base_url = "https://stats.espncricinfo.com/ci/engine/stats/index.html"

    # Header to prevent Error 403 permission denied
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    # Init while loop setting - storage, page num etc.
    table_store = {}
    page_num = 1
    theend = False

    # Pagination
    while not theend and page_num <= max_pages:

        # Modify URL based on type of info selected
        url = f"{base_url}?class={matchclass}{team_text};page={page_num};template=results;type={activity}{view_text};wrappertype=print"

        # Send request and check errors
        page = requests.get(url, headers=headers)

        if not page.ok:
            raise requests.exceptions.HTTPError(
                f"HTTP Error {page.status_code}: Unable to fetch data from {url}"
            )

        # Read table from HTML - safely
        try:
            tables = pd.read_html(StringIO(page.text))
            if len(tables) < 3:
                raise RuntimeError("Unexpected HTML structure: Data table not found")
            data = tables[2]
        except ValueError:
            raise RuntimeError("Failed to parse HTML")

        # # Make everything string for now
        # data = data.astype(str)

        # Check if extract data is empty and break if so
        if data.shape == (1, 1):
            if page == 1:
                raise RuntimeError(
                    f"No data available for {activity} in {matchtype} matches"
                )
            break

        # Append data
        table_store[page_num] = data
        page_num += 1

    # Concat
    scraped = pd.concat(table_store).reset_index(drop=True)

    return scraped


# %%
inning = fetch_cricinfo(
    matchtype="test", sex="women", activity="fielding", view="innings"
)
career = fetch_cricinfo(matchtype="test", sex="men", activity="fielding", view="career")

# %%

df = career

# Cast to string then replace
df = df.replace("-", np.nan)

# Drop and/or rename columns
df.columns = df.columns.str.lower()
if "unnamed: 8" in df.columns:
    df = df.drop(columns="unnamed: 8")

df = df.rename(
    columns={
        "mat": "matches",
        "inns": "innings",
        "start_date": "date",
        "dis": "dismissals",
        "ct": "caught",
        "st": "stumped",
        "ct wk": "caught_behind",
        "ct fi": "caught_fielder",
        "md": "max_dismissals_innings",
        "d/i": "dismissals_innings",
    }
)


# %%


# Innings view
if "matches" not in df.columns:
    df["opposition"] = df["opposition"].str.replace("v | Women| Wmn", "", regex=True)
    df["opposition"] = df["opposition"].apply(rename_country)

# Add depending on columns
if "span" in df.columns:
    df["start"] = df["span"].str.split("-").str[0]
    df["end"] = df["span"].str.split("-").str[1]

# Participation status - init to b
if "overs" in df.columns:
    df["participation"] = participation_status(df, "overs")
    # Impute non-balling overs to 0
    df["overs"] = np.where(df["participation"] != "b", np.nan, df["overs"])

# Clean dtypes
df = dtype_clean(df)

# Calculate average and avoid rounding issues
df["average"] = df["runs"] / df["wickets"]

# Calculate (approximate) balls
if "balls" not in df.columns:
    df["balls"] = np.trunc(df["overs"]) * 6 + (df["overs"] % 1) * 10

# Recompute economy to avoid rounding issues
# Do not recompute if difference is too large
threshold = 0.05
economy = df["runs"] / (df["balls"] / 6)
different = np.abs(round(economy, 2) - df["economy"]) > threshold
if "economy" in df.columns:
    df["economy"] = np.where(different, df["economy"], economy)
else:
    df["economy"] = economy

# (Re)Compute strike rate
df["strike_rate"] = df["balls"] / df["wickets"]

# Extract country
if df["player"].str.contains(r"\(", regex=True).any():
    df["country"] = df["player"].str.extract(r"\(([a-zA-Z /\-]+)\)")
    df["country"] = df["country"].str.replace(r"-W", "", regex=True)
    df["country"] = df["country"].apply(rename_country)
    df["player"] = (
        df["player"].str.replace(r"\([a-zA-Z /\-]+\)", "", regex=True).str.strip()
    )

# Reorder vars
if "matches" in df.columns:
    cols_order = [
        "player",
        "country",
        "start",
        "end",
        "matches",
        "innings",
        "overs",
        "balls",
        "maidens",
        "runs",
        "wickets",
        "average",
        "economy",
        "strike_rate",
        "best_bowling_innings",
        "best_bowling_match",
        "four_wickets",
        "five_wickets",
        "ten_wickets",
    ]
else:
    cols_order = [
        "date",
        "player",
        "country",
        "overs",
        "balls",
        "maidens",
        "runs",
        "wickets",
        "average",
        "economy",
        "strike_rate",
        "innings",
        "participation",
        "opposition",
        "ground",
    ]
cols_order = [col for col in cols_order if col in df.columns]
df = df[cols_order]
