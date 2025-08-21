"""
Loaders for EIA state-level monthly production (Crude Oil and Natural Gas).

- Reads CSV or Excel exported from EIA with columns like:
  Month, "<State> Crude Oil (Thousand Barrels per Day)  thousand barrels per day", ...
  Month, "<State> Natural Gas Gross Withdrawals (Million Cubic Feet per Day)  million cubic feet per day", ...

- Always converts daily rates to MONTHLY volumes:
  * oil_bbl = kbpd * 1_000 * days_in_month
  * gas_mcf = MMcf/d * 1_000 * days_in_month

- Returns tidy DataFrames:
  * load_crude_oil(...)   -> ['period_month','state_name','oil_bbl']
  * load_natural_gas(...) -> ['period_month','state_name','gas_mcf']
  * load_eia_pair(...)    -> merge on ['period_month','state_name']
"""

from pathlib import Path
from typing import Optional, Iterable, Literal

import pandas as pd

from ..transforms.common import parse_month_to_ymd_first_day, days_in_period_month

# Aggregates in EIA data
EXCLUDE_AGGREGATES = {
    "U.S.",
    "Federal Offshore Gulf of America",
    "Federal Offshore Pacific",
    "Other States"
}

# Internal helpers

def _read_any(path: Path) -> pd.DataFrame:
    """Read CSV or Excel transparently."""
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from header names."""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def _melt_state_columns(df: pd.DataFrame, value_col_name: str) -> pd.DataFrame:
    """
    Melt from wide (Month + many state columns) to long with:
      ['Month', 'state_col', value_col_name]
    """
    return df.melt(id_vars=["Month"], var_name="state_col", value_name=value_col_name)

def _extract_state_from_col(col: pd.Series) -> pd.Series:
    """
    EIA headers examples:
      'West Virginia Crude Oil (Thousand Barrels per Day)  thousand barrels per day'
      'Pennsylvania Natural Gas Gross Withdrawals (Million Cubic Feet per Day)  million cubic feet per day'
    Remove the metric part and keep only the state/area name.
    """
    return (
        col
        .str.replace(r"\s*Crude Oil.*$", "", regex=True)
        .str.replace(r"\s*Natural Gas.*$", "", regex=True)
        .str.strip()
    )

def _filter_states(s: pd.Series, include_states: Optional[Iterable[str]]) -> pd.Series:
    """Apply include filter (if provided) and drop known aggregates."""
    out = s.copy()
    if include_states:
        keep = set(include_states)
        out = out[out.isin(keep)]
    # remove aggregates
    out = out[~out.isin(EXCLUDE_AGGREGATES)]
    return out

# Public loaders

def load_crude_oil(
    path: Path,
    include_states: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Load EIA crude oil (monthly volumes).

    Returns columns:
      ['period_month', 'state_name', 'oil_bbl']
      - 'period_month' = 'YYYY-MM-01'
      - 'oil_bbl'      = barrels per month
    """
    df = _normalize_columns(_read_any(path))
    if "Month" not in df.columns:
        raise ValueError("EIA crude oil file must contain a 'Month' column.")

    # Wide -> Long
    long_df = _melt_state_columns(df, value_col_name="oil_kbpd")
    long_df["state_name"] = _extract_state_from_col(long_df["state_col"])

    # Filter states and exclude aggregates
    mask = _filter_states(long_df["state_name"], include_states).index
    long_df = long_df.loc[mask].copy()

    # Normalize month to 'YYYY-MM-01'
    long_df["period_month"] = long_df["Month"].map(parse_month_to_ymd_first_day)

    # Convert kbpd -> barrels per month
    def kbpd_to_bbl(period: str, kbpd: Optional[float]) -> Optional[float]:
        if pd.isna(kbpd):
            return None
        return float(kbpd) * 1_000.0 * days_in_period_month(period)

    long_df["oil_bbl"] = [
        kbpd_to_bbl(p, k) for p, k in zip(long_df["period_month"], long_df["oil_kbpd"])
    ]

    out = long_df[["period_month", "state_name", "oil_bbl"]].copy()
    return out.sort_values(["state_name", "period_month"]).reset_index(drop=True)


def load_natural_gas(
    path: Path,
    include_states: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Load EIA natural gas (monthly volumes).

    Returns columns:
      ['period_month', 'state_name', 'gas_mcf']
      - 'period_month' = 'YYYY-MM-01'
      - 'gas_mcf'      = thousand cubic feet per month
    """
    df = _normalize_columns(_read_any(path))
    if "Month" not in df.columns:
        raise ValueError("EIA natural gas file must contain a 'Month' column.")

    # Wide -> Long
    long_df = _melt_state_columns(df, value_col_name="gas_mmcfd")
    long_df["state_name"] = _extract_state_from_col(long_df["state_col"])

    # Filter states and exclude aggregates
    mask = _filter_states(long_df["state_name"], include_states).index
    long_df = long_df.loc[mask].copy()

    # Normalize month to 'YYYY-MM-01'
    long_df["period_month"] = long_df["Month"].map(parse_month_to_ymd_first_day)

    # Convert MMcf/d -> mcf per month
    def mmcfd_to_mcf(period: str, mmcfd: Optional[float]) -> Optional[float]:
        if pd.isna(mmcfd):
            return None
        return float(mmcfd) * 1_000.0 * days_in_period_month(period)

    long_df["gas_mcf"] = [
        mmcfd_to_mcf(p, g) for p, g in zip(long_df["period_month"], long_df["gas_mmcfd"])
    ]

    out = long_df[["period_month", "state_name", "gas_mcf"]].copy()
    return out.sort_values(["state_name", "period_month"]).reset_index(drop=True)


def load_eia_pair(
    oil_path: Path,
    gas_path: Path,
    include_states: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Merge crude oil + natural gas (monthly volumes) on ['period_month','state_name'].

    Returns columns:
      ['period_month','state_name','oil_bbl','gas_mcf']
    """
    oil = load_crude_oil(oil_path, include_states=include_states)
    gas = load_natural_gas(gas_path, include_states=include_states)

    out = pd.merge(
        oil, gas,
        on=["period_month", "state_name"],
        how="outer",
        validate="one_to_one"
    )
    return out.sort_values(["state_name", "period_month"]).reset_index(drop=True)
