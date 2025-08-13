import json
import math
from typing import Any, Dict, List, Tuple
import pandas as pd

JOINER_FOR_SCALARS = "|"
MAX_ROWS_SOFT_CAP = 1_000_000

def _is_list_of_dicts(x: Any) -> bool:
    return isinstance(x, list) and len(x) > 0 and all(isinstance(i, dict) for i in x)

def _is_list_of_scalars(x: Any) -> bool:
    if not isinstance(x, list):
        return False
    if len(x) == 0:
        return True
    return all(not isinstance(i, (list, dict)) for i in x)

def _flatten_dict(d: Dict[str, Any], parent: str = "") -> Dict[str, Any]:
    items = {}
    for k, v in d.items():
        key = f"{parent}.{k}" if parent else k
        if isinstance(v, dict):
            items.update(_flatten_dict(v, key))
        else:
            items[key] = v
    return items

def _explode_list_of_dicts(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df = df.explode(col, ignore_index=True)
    df[col] = df[col].apply(lambda x: {} if (isinstance(x, float) and math.isnan(x)) else x)
    norm = pd.json_normalize(df[col].where(df[col].notna(), {}))
    norm.columns = [f"{col}.{c}" for c in norm.columns]
    df = df.drop(columns=[col]).reset_index(drop=True)
    df = pd.concat([df, norm], axis=1)
    return df

def _join_list_of_scalars(df: pd.DataFrame, col: str) -> pd.DataFrame:
    def _join(x):
        if x is None:
            return ""
        if isinstance(x, list):
            return JOINER_FOR_SCALARS.join("" if y is None else str(y) for y in x)
        return x
    df[col] = df[col].apply(_join)
    return df

def _normalize_lists(df: pd.DataFrame) -> pd.DataFrame:
    while True:
        list_dict_cols = [c for c in df.columns if df[c].apply(_is_list_of_dicts).any()]
        if list_dict_cols:
            for c in list_dict_cols:
                df = _explode_list_of_dicts(df, c)
                if len(df) > MAX_ROWS_SOFT_CAP:
                    raise RuntimeError(
                        f"Row explosion exceeded {MAX_ROWS_SOFT_CAP} rows while expanding '{c}'."
                    )
            continue

        list_scalar_cols = [c for c in df.columns if df[c].apply(_is_list_of_scalars).any()]
        if list_scalar_cols:
            for c in list_scalar_cols:
                df = _join_list_of_scalars(df, c)
            continue
        break
    return df

def json_to_single_csv_records(obj: Any) -> pd.DataFrame:
    if isinstance(obj, list):
        base_rows = []
        for item in obj:
            if isinstance(item, dict):
                base_rows.append(_flatten_dict(item))
            else:
                base_rows.append({"value": item})
        df = pd.DataFrame(base_rows if base_rows else [{}])
    elif isinstance(obj, dict):
        df = pd.DataFrame([_flatten_dict(obj)])
    else:
        return pd.DataFrame([{"value": obj}])

    df = _normalize_lists(df)

    for c in df.columns:
        df[c] = df[c].apply(lambda x: "" if x is None or (isinstance(x, float) and math.isnan(x))
                            else (json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x))
    df.columns = [c.replace("\n", " ").strip() for c in df.columns]
    return df

def recursive_json_to_csv(json_input_path: str, csv_output_path: str) -> Tuple[int, List[str]]:
    with open(json_input_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    df = json_to_single_csv_records(obj)
    df.fillna("", inplace=True)
    df.to_csv(csv_output_path, index=False, encoding="utf-8")
    return len(df), list(df.columns)
