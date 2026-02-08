import pandas as pd

def load_excel(path: str, sheet_name: str = "tbl_MY") -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name)
    df.columns = df.columns.str.strip()
    return df
