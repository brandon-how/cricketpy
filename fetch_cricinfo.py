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
test = fetch_cricinfo(
        matchtype="test",
        sex="women",
        activity="batting",
        view="innings"
         )
# %%
df = test

# %%

def clean_batting_data(df: pd.DataFrame) -> pd.DataFrame:
    # Cast to string then replace
    df = df.replace("-", np.nan)
    # Rename columns 
    df.columns = df.columns.str.lower()
    df = df.rename(
        columns={
        "Mat": "matches",
        "Inns": "innings",
            "NO": "not_outs",
            "HS": "highscore",
            "Ave": "average",
            "100": "hundreds",
            "50": "fifties",
            "0": "ducks",
            "SR": "strike_rate",
            "BF": "balls_faced",
            "4s": "fours",
            "6s": "sixes",
            "Mins": "minutes",
            "Start Date": "date"
        }
    )

    if "matches" in df.columns:
        # Add columns
        df["highscore_notout"] = df["highscore"].str.contains("\\*")
        if "span" in df.columns:
            df["start"] = df['span'].str.split('-').str[0]
            df["end"] = df['span'].str.split('-').str[1]
        # Clean column
        df["highscore"] = df["highscore"].str.replace("*", "")
        # Transform dtypes
        df = dtype_clean(df)
        # Batting average
        df["average"] = df["runs"] / (df["innings"] - df["not_outs"])
    else:




clean_batting_data <- function(x) {

  } else {
    x$Innings <- as.integer(x$Innings)
    x$Minutes <- as.numeric(x$Minutes)
    x$Date <- lubridate::dmy(x$Date)
    x$Opposition <- stringr::str_replace_all(x$Opposition, "v | Women| Wmn", "")
    x$Opposition <- rename_countries(x$Opposition)
    # Add not out and participation column and remove annotations from runs
    x$NotOut <- seq(NROW(x)) %in% grep("*", x$Runs, fixed = TRUE)
    x$Runs <- gsub("*", "", x$Runs, fixed = TRUE)
    x$Participation <- participation_status(x$Runs)
    x$Runs[x$Participation != "B"] <- NA
    x$Runs <- as.integer(x$Runs)
  }

  if ("BallsFaced" %in% vars) {
    x$BallsFaced <- as.integer(x$BallsFaced)
    x$StrikeRate <- x$Runs / x$BallsFaced * 100
  }
  if ("Fours" %in% vars) {
    x$Fours <- as.integer(x$Fours)
    x$Sixes <- as.integer(x$Sixes)
  }

  # Extract country information if it is present
  # This should only be required when multiple countries are included
  country <- (length(grep("\\(", x$Player) > 0))
  if (country) {
    x$Country <- stringr::str_extract(x$Player, "\\([a-zA-Z /\\-extends]+\\)")
    x$Country <- stringr::str_replace_all(x$Country, "\\(|\\)|-W", "")
    x$Country <- rename_countries(x$Country)
    x$Player <- stringr::str_replace(x$Player, "\\([a-zA-Z /\\-]+\\)", "")
  }

  # Re-order and select columns
  vars <- colnames(x)
  if (career) {
    varorder <- c(
      "Player", "Country", "Start", "End", "Matches", "Innings", "NotOuts", "Runs", "HighScore", "HighScoreNotOut",
      "Average", "BallsFaced", "StrikeRate", "Hundreds", "Fifties", "Ducks", "Fours", "Sixes"
    )
  } else {
    varorder <- c(
      "Date", "Player", "Country", "Runs", "NotOut", "Minutes", "BallsFaced", "Fours", "Sixes",
      "StrikeRate", "Innings", "Participation", "Opposition", "Ground"
    )
  }
  varorder <- varorder[varorder %in% vars]

  return(x[, varorder])
}


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
