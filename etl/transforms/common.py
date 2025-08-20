import calendar
import pandas as pd

def parse_month_to_ymd_first_day(x) -> str:
    """Return 'YYYY-MM-01' for any parseable date-like value."""
    dt = pd.to_datetime(x)
    return dt.to_period("M").to_timestamp().date().isoformat()

def days_in_period_month(period_ymd: str) -> int:
    y, m = int(period_ymd[:4]), int(period_ymd[5:7])
    return calendar.monthrange(y, m)[1]

def to_float_or_none(x):
    try:
        if pd.isna(x) or str(x).strip() == "":
            return None
        return float(x)
    except Exception:
        return None

def to_date_iso_or_none(x):
    try:
        if pd.isna(x) or str(x).strip() == "":
            return None
        return pd.to_datetime(x).date().isoformat()
    except Exception:
        return None
