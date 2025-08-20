import pandas as pd
from pathlib import Path
from typing import Optional
from ..transforms.common import parse_month_to_ymd_first_day, days_in_period_month

# Only the states required by the challenge
TARGET_STATES = {"West Virginia", "Pennsylvania"}

# --- Internal helpers ---------------------------------------------------------

def _read_any(path: Path) -> pd.DataFrame:
    """Read CSV or Excel transparently."""
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Basic column normalization: strip spaces in header names."""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def _melt_state_columns(df: pd.DataFrame, value_col_name: str) -> pd.DataFrame:
    """
    Melt from wide (Month + state columns) to long:
    returns ['Month', 'state_col', value_col_name]
    """
    return df.melt(id_vars=["Month"], var_name="state_col", value_name=value_col_name)

def _extract_state_from_col(col: pd.Series) -> pd.Series:
    """
    EIA state columns usually look like:
      'West Virginia Crude Oil (Thousand Barrels per Day)  thousand barrels per day'
      'Pennsylvania Natural Gas Gross Withdrawals (Million Cubic Feet per Day)  million cubic feet per day'
    We remove the metric part and keep the clean state name.
    """
    return (
        col
        .str.replace(r"\s*Crude Oil.*$", "", regex=True)
        .str.replace(r"\s*Natural Gas.*$", "", regex=True)
        .str.strip()
    )

# --- Public loaders -----------------------------------------------------------

def load_crude_oil(path: Path) -> pd.DataFrame:
    """
    Load EIA crude oil by state.
    Input columns example:
      Month, 'West Virginia Crude Oil (Thousand Barrels per Day)  thousand barrels per day', ...
    Output columns: ['period_month','state_name','oil_bbl']
      - 'period_month' = 'YYYY-MM-01'
      - 'oil_bbl' = barrels per month (converted from kbpd)
    """
    df = _normalize_columns(_read_any(path))

    if "Month" not in df.columns:
        raise ValueError("EIA crude oil file must contain a 'Month' column.")

    long_df = _melt_state_columns(df, value_col_name="oil_kbpd")
    long_df["state_name"] = _extract_state_from_col(long_df["state_col"])
    long_df = long_df[long_df["state_name"].isin(TARGET_STATES)].copy()

    # Month -> 'YYYY-MM-01'
    long_df["period_month"] = long_df["Month"].map(parse_month_to_ymd_first_day)

    # kbpd -> barrels per month
    def kbpd_to_bbl(period: str, kbpd: Optional[float]) -> Optional[float]:
        if pd.isna(kbpd):
            return None
        return float(kbpd) * 1_000.0 * days_in_period_month(period)

    long_df["oil_bbl"] = [
        kbpd_to_bbl(p, k) for p, k in zip(long_df["period_month"], long_df["oil_kbpd"])
    ]

    out = long_df[["period_month", "state_name", "oil_bbl"]].copy()
    out = out.sort_values(["state_name", "period_month"]).reset_index(drop=True)
    return out


def load_natural_gas(path: Path) -> pd.DataFrame:
    """
    Load EIA natural gas gross withdrawals by state.
    Input columns example:
      Month, 'West Virginia Natural Gas Gross Withdrawals (Million Cubic Feet per Day)  million cubic feet per day', ...
    Output columns: ['period_month','state_name','gas_mcf']
      - 'period_month' = 'YYYY-MM-01'
      - 'gas_mcf' = thousand cubic feet per month (converted from MMcf/d)
        (1 MMcf = 1,000 mcf)
    """
    df = _normalize_columns(_read_any(path))

    if "Month" not in df.columns:
        raise ValueError("EIA natural gas file must contain a 'Month' column.")

    long_df = _melt_state_columns(df, value_col_name="gas_mmcfd")
    long_df["state_name"] = _extract_state_from_col(long_df["state_col"])
    long_df = long_df[long_df["state_name"].isin(TARGET_STATES)].copy()

    # Month -> 'YYYY-MM-01'
    long_df["period_month"] = long_df["Month"].map(parse_month_to_ymd_first_day)

    # MMcf/d -> mcf per month
    # Explanation:
    #   mmcfd (million cubic feet per day) * 1,000 mcf/MMcf * days_in_month
    def mmcfd_to_mcf(period: str, mmcfd: Optional[float]) -> Optional[float]:
        if pd.isna(mmcfd):
            return None
        return float(mmcfd) * 1_000.0 * days_in_period_month(period)

    long_df["gas_mcf"] = [
        mmcfd_to_mcf(p, g) for p, g in zip(long_df["period_month"], long_df["gas_mmcfd"])
    ]

    out = long_df[["period_month", "state_name", "gas_mcf"]].copy()
    out = out.sort_values(["state_name", "period_month"]).reset_index(drop=True)
    return out


def load_eia_pair(oil_path: Path, gas_path: Path) -> pd.DataFrame:
    """
    Load and merge crude oil + natural gas into a single DataFrame:
    Columns: ['period_month','state_name','oil_bbl','gas_mcf']
    (Input for the fact table loader)
    """
    oil = load_crude_oil(oil_path)
    gas = load_natural_gas(gas_path)

    # Full outer join on (period_month, state_name) to be resilient to missing months in either series
    out = pd.merge(
        oil, gas, on=["period_month", "state_name"], how="outer", validate="one_to_one"
    )

    # Sort for deterministic outputs
    out = out.sort_values(["state_name", "period_month"]).reset_index(drop=True)
    return out
