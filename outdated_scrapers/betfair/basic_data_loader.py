from typing import Any, Optional
import os
import pandas as pd
from urllib.request import urlopen
from io import StringIO, BytesIO


def load_data_to_df(source: Any, fmt: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Load data into a pandas DataFrame.

    Parameters:
    - source: one of
        - pandas.DataFrame -> returned as a copy
        - str -> path to a file OR URL (csv, json, parquet, xlsx). extension is used if fmt is None
        - list or dict -> converted with pd.DataFrame(...)
    - fmt: optional override for file format: 'csv', 'json', 'parquet', 'excel'
    - kwargs: forwarded to the underlying pandas reader (pd.read_csv, pd.read_json, ...)

    Returns:
    - pd.DataFrame

    Raises:
    - ValueError for unknown file types
    - TypeError for unsupported source types
    """
    # If already a DataFrame, return a copy to avoid side effects
    if isinstance(source, pd.DataFrame):
        return source.copy()

    # Simple python-native containers -> DataFrame
    if isinstance(source, (list, dict)):
        return pd.DataFrame(source)

    # If a string, treat as file path or URL
    if isinstance(source, str):
        # Check if it's a URL
        is_url = source.startswith(('http://', 'https://'))

        # determine format from explicit fmt or file extension
        if fmt is None:
            _, ext = os.path.splitext(source.split('?')[0])  # strip query params for URL
            if not ext:
                raise ValueError("No file extension found; pass fmt explicitly (e.g. fmt='csv').")
            fmt = ext.lstrip('.').lower()
        else:
            fmt = fmt.lower()

        # For URLs, fetch content first
        if is_url:
            with urlopen(source) as response:
                content = response.read()

            if fmt in ("csv", "txt"):
                return pd.read_csv(StringIO(content.decode('utf-8')), **kwargs)
            if fmt in ("json",):
                return pd.read_json(BytesIO(content), **kwargs)
            if fmt in ("parquet", "pq"):
                return pd.read_parquet(BytesIO(content), **kwargs)
            if fmt in ("xlsx", "xls", "excel"):
                return pd.read_excel(BytesIO(content), **kwargs)
        else:
            # Local file path
            if fmt in ("csv", "txt"):
                return pd.read_csv(source, **kwargs)
            if fmt in ("json",):
                return pd.read_json(source, **kwargs)
            if fmt in ("parquet", "pq"):
                return pd.read_parquet(source, **kwargs)
            if fmt in ("xlsx", "xls", "excel"):
                return pd.read_excel(source, **kwargs)

        raise ValueError(f"Unsupported file format: {fmt!r}. Supported: csv, json, parquet, excel.")

    raise TypeError(f"Unsupported source type: {type(source)}. Provide DataFrame, list/dict, or file path.")


def fetch_betfair_html(url: str = "https://www.betfair.ro/sport") -> str:
    """Fetch HTML content from Betfair URL using urllib."""
    with urlopen(url) as response:
        return response.read().decode('utf-8')


# Add a simple CLI entrypoint to execute the loader with an attached file path
if __name__ == "__main__":
    # Example 1: Load local JSON file
    df = load_data_to_df(source='/home/adam/PycharmProjects/Betting/betfair/football-basic-sample.json')
    print("Local file DataFrame:")
    print(df.head())

    # Example 2: Fetch Betfair HTML
    print("\n\nFetching Betfair HTML:")
    html_content = fetch_betfair_html()
    print(f"Fetched {len(html_content)} characters from Betfair")
    print(html_content[:500])  # Print first 500 chars
