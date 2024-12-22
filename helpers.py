import pandas as pd
import numpy as np


def col_string_to_float(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Converts a specified column in a DataFrame to float.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame containing the column to convert.
    col : str
        The name of the column to convert to float.

    Returns:
    -------
    pd.Series
        The column converted to float if possible. If conversion fails, the original column is returned.
    """
    try:
        new_col = df[col].astype(float)
        return new_col
    except ValueError:
        return df[col]


def string_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all string or object-type columns in a DataFrame to floats, handling empty strings as NaN.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame with columns to convert.

    Returns:
    -------
    pd.DataFrame
        The DataFrame with applicable columns converted to float.
    """
    for col in df.select_dtypes(include=["string", "object"]).columns:
        df[col] = df[col].str.strip().replace("", np.nan)
        df[col] = col_string_to_float(df, col)
        return df


def float_to_int(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all float columns in a DataFrame to integers if all values in the column are whole numbers.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame with columns to check and convert.

    Returns:
    -------
    pd.DataFrame
        The DataFrame with applicable float columns converted to int.
    """
    for col in df.select_dtypes(include=["float"]).columns:
        if df[col].apply(float.is_integer).all():
            df[col] = df[col].astype(int)
    return df


def date_to_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all columns in a DataFrame with 'date' in their names to datetime format.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame containing columns to convert.

    Returns:
    -------
    pd.DataFrame
        The DataFrame with applicable columns converted to datetime.
    """
    for col in df.filter(like="date").columns:
        df[col] = pd.to_datetime(df[col])
    return df
