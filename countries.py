import re

men = {
    1: "England",
    2: "Australia",
    3: "South Africa",
    4: "West Indies",
    5: "New Zealand",
    6: "India",
    7: "Pakistan",
    8: "Sri Lanka",
    9: "Zimbabwe",
    11: "United States of America",
    12: "Bermuda",
    14: "East Africa",
    15: "Netherlands",
    17: "Canada",
    19: "Hong Kong",
    20: "Papua New Guinea",
    25: "Bangladesh",
    26: "Kenya",
    27: "United Arab Emirates",
    28: "Namibia",
    29: "Ireland",
    30: "Scotland",
    32: "Nepal",
    37: "Oman",
    40: "Afghanistan",
}

women = {
    289: "Australia",
    1026: "England",
    1863: "India",
    4240: "Bangladesh",
    3379: "South Africa",
    3672: "Sri Lanka",
    2614: "New Zealand",
    2285: "Ireland",
    3022: "Pakistan",
    2461: "Netherlands",
    3867: "West Indies",
    825: "Denmark",
    3808: "Jamaica",
    2331: "Japan",
    3505: "Scotland",
    3843: "Trinidad & Tobago",
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
        (r"PNG|P\\.N\\.G\\.", "Papua New Guinea"),
        (r"^SA", "South Africa"),
        (r"SCOT|SCO|Scot$", "Scotland"),
        (r"SL", "Sri Lanka"),
        (r"TTWmn|T & T", "Trinidad and Tobago"),
        (r"UAE|U\\.A\\.E\\.", "United Arab Emirates"),
        (r"USA|U\\.S\\.A\\.", "United States of America"),
        (r"World$|World-XI", "World XI"),
        (r"WI", "West Indies"),
        (r"YEWmn|Y\\. Eng", "Young England"),
        (r"ZIM", "Zimbabwe"),
    ]

    # Search through each pattern and replacement
    for pattern, replacement in patterns:
        country = re.sub(pattern, replacement, country)

    return country
