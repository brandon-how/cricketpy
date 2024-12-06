import re

men = {
    "England": 1,
    "Australia": 2,
    "South Africa": 3,
    "West Indies": 4,
    "New Zealand": 5,
    "India": 6,
    "Pakistan": 7,
    "Sri Lanka": 8,
    "Zimbabwe": 9,
    "United States of America": 11,
    "Bermuda": 12,
    "East Africa": 14,
    "Netherlands": 15,
    "Canada": 17,
    "Hong Kong": 19,
    "Papua New Guinea": 20,
    "Bangladesh": 25,
    "Kenya": 26,
    "United Arab Emirates": 27,
    "Namibia": 28,
    "Ireland": 29,
    "Scotland": 30,
    "Nepal": 32,
    "Oman": 37,
    "Afghanistan": 40,
}


women = {
    "Australia": 289,
    "England": 1026,
    "India": 1863,
    "Bangladesh": 4240,
    "South Africa": 3379,
    "Sri Lanka": 3672,
    "New Zealand": 2614,
    "Ireland": 2285,
    "Pakistan": 3022,
    "Netherlands": 2461,
    "West Indies": 3867,
    "Denmark": 825,
    "Jamaica": 3808,
    "Japan": 2331,
    "Scotland": 3505,
    "Trinidad & Tobago": 3843,
}


def rename_country(country: str) -> str:
    """
    Standardizes and replaces country abbreviations or shorthand with their full names.

    Args:
        country (str): A country name or abbreviation (e.g., "AFG", "AUS", "ENG") to be renamed.

    Returns:
        str: The standardized country name (e.g., "Afghanistan", "Australia", "England").
    """
    # Define a series of patterns and their replacements
    patterns = [
        (r"AFG", "Afghanistan"),
        (r"Afr$", "Africa XI"),
        (r"AUS", "Australia"),
        (r"Bdesh|BDESH|BD", "Bangladesh"),
        (r"BMUDA", "Bermuda"),
        (r"CAN", "Canada"),
        (r"DnWmn|Denmk", "Denmark"),
        (r"EAf", "East (and Central) Africa"),
        (r"ENG", "England"),
        (r"HKG", "Hong Kong"),
        (r"ICC$", "ICC World XI"),
        (r"INDIA|IND", "India"),
        (r"IntWn|Int XI", "International XI"),
        (r"Ire$|IRELAND|IRE", "Ireland"),
        (r"JamWn", "Jamaica"),
        (r"JPN", "Japan"),
        (r"KENYA", "Kenya"),
        (r"NAM", "Namibia"),
        (r"NEPAL", "Nepal"),
        (r"Neth$|NL", "Netherlands"),
        (r"NZ", "New Zealand"),
        (r"OMAN", "Oman"),
        (r"PAK", "Pakistan"),
        (r"PNG|P\.N\.G\.", "Papua New Guinea"),
        (r"^SA", "South Africa"),
        (r"SCOT|SCO|Scot$", "Scotland"),
        (r"SL", "Sri Lanka"),
        (r"TTWmn|T & T", "Trinidad and Tobago"),
        (r"UAE|U\.A\.E\.", "United Arab Emirates"),
        (r"USA|U\.S\.A\.", "United States of America"),
        (r"World$|World-XI", "World XI"),
        (r"WI", "West Indies"),
        (r"YEWmn|Y\. Eng", "Young England"),
        (r"ZIM", "Zimbabwe"),
    ]

    # Search through each pattern and replacement
    for pattern, replacement in patterns:
        country = re.sub(pattern, replacement, country)

    return country
