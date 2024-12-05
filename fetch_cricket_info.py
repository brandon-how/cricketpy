# %%
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
from math import floor
from pathlib import Path
from dotenv import load_dotenv

# Set environment
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)
env = os.getenv("env")

# %%

url = "https://stats.espncricinfo.com/ci/engine/stats/index.html?class=1;page=1;size=200;template=results;type=batting;wrappertype=print"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

page = requests.get(url, headers=headers)
soup = BeautifulSoup(page.content, "html.parser")


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
