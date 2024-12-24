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
    output = df.copy()
    # Initial go
    # for col in output.columns:
    #     output[col] = col_string_to_float(output, col)

    # Second loop
    for col in output.select_dtypes(include=["string", "object"]).columns:
        output[col] = output[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )
        output[col] = output[col].replace("", np.nan)
        output[col] = col_string_to_float(output, col)
    return output


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
    output = df.copy()
    for col in output.select_dtypes(include=["float"]).columns:
        if output[col].apply(float.is_integer).all():
            output[col] = output[col].astype(int)
    return output


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
    output = df.copy()
    for col in output.filter(like="date").columns:
        output[col] = pd.to_datetime(output[col])
    return output


def dtype_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and optimizes the data types in a DataFrame.

    This function performs a series of type conversions on the input DataFrame:
    1. Converts string or object-type columns to float where possible.
    2. Converts float columns to integer if all values in the column are whole numbers.
    3. Converts columns with 'date' in their names to datetime format.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to be cleaned and optimized.

    Returns:
    -------
    pd.DataFrame
        The DataFrame with cleaned and optimized data types.
    """
    # String to float
    output = df.copy()
    output = string_to_float(df)
    # Float to int
    output = float_to_int(output)
    # Date to date
    output = date_to_date(output)
    return output
