# %%
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
from math import floor
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urljoin
from countries import *
from difflib import get_close_matches
from io import StringIO


# Set environment
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)
env = os.getenv("env")

# %%
# Set up for test cases
matchtype = "test"  # ["test", "odi", "t20"]
sex = "men"  # ["men", "women"]
country = None
activity = "batting"  # ["batting", "bowling", "fielding"]
view = "career"  # ["career", "innings"]

# Transform everything to lower case
matchtype = matchtype.lower()
sex = sex.lower()
activity = activity.lower()
view = view.lower()

# View text
view_text = ";view=innings" if view == "innings" else ""

# URL signifier for match type
matchclass = ["test", "odi", "t20"].index(matchtype) + 1 + 7 * (sex == "women")

# Team number matching for input country
if country is not None:
    country_sex = men if sex == "men" else women
    # Get closest country match
    country_match = get_close_matches(
        country.lower(), [x.lower() for x in country_sex.keys()], n=1, cutoff=0.5
    )
    if len(country_match) == 0 or country_match[0].title() not in country_sex.keys():
        raise ValueError("Country not found")
    # Get team code and team URL segment
    team = country_sex[country_match[0].title()]
    team_text = f";team={team}"
else:
    team_text = ""

# Define base URL
base_url = "https://stats.espncricinfo.com/ci/engine/stats/index.html"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

# Table store
table_store = {}

# Start page
page_num = 1

theend = False
while not theend:

    # Modify URL based on type of info selected
    url = f"{base_url}?class={matchclass}{team_text};page={page_num};template=results;type={activity}{view_text};wrappertype=print"

    # Send request and check errors
    page = requests.get(url, headers=headers)

    if not page.ok:
        raise RuntimeError(f"Error {page.status_code} in URL")

    # soup = BeautifulSoup(page.content, "html.parser")

    # Read table from HTML
    tables = pd.read_html(StringIO(page.text))
    data = tables[2]

    # Make everything string for now
    data = data.astype(str)

    # Check if extract data is empty
    if data.shape == (1, 1):
        if page == 1:
            raise RuntimeError("No data available")
        break

    # Append data
    table_store[page_num] = data
    page_num += 1

# Concat
scraped = pd.concat(table_store).reset_index(drop=True)
