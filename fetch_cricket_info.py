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

# Start page
page = 1

# %%
# Modify URL based on type of info selected
url = f"{base_url}?class={matchclass}{team_text};page={page};template=results;type={activity}{view_text};wrappertype=print"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

page = requests.get(url, headers=headers)
soup = BeautifulSoup(page.content, "html.parser")
page.ok

# %%
# Get url


# %%
def webscrape_article(
    url: str,
) -> pd.DataFrame:
    """
    For a given CIDRAP article URL get all the data

    Args:
        url (str): URL of the CIDRAP article to scrape

    Returns:
        pd.DataFrame: dataframe of the data
    """

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    # Some article pages are same day pages, that render other articles below main content which will be parsed with their own URL
    div_to_delete = soup.find("section", class_="region-below-content")
    # Check if the element exists and delete it and its children
    if div_to_delete:
        div_to_delete.decompose()

    title = soup.find("h1").text.strip()
    author = (
        soup.select_one(".field--name-field-bio-name a").text.strip()
        if soup.select_one(".field--name-field-bio-name a")
        else None
    )
    datetime = soup.find("time")["datetime"]
    # Topic not always present. add for fault tolerance
    topic_element = soup.select_one(".field--name-field-related-topics a")
    topic = topic_element.text.strip() if topic_element else None

    # Extract all paragraph texts
    article_text_elements = soup.select(".group-right .field--name-field-body p")
    # article_text = [p.get_text(strip=True) for p in article_text_elements]
    article_text = " ".join(p.get_text(strip=True) for p in article_text_elements)

    # Create DataFrame
    data = {
        "title": [title],
        "author": [author],
        "date_time": [datetime],
        "topic": [topic],
        "article_text": [article_text],
        "article_url": url,
        "article_source": "cidrap",
    }

    df = pd.DataFrame(data)

    return df
