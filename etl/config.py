from pathlib import Path

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
DB_PATH = Path("data/oilgas.db")

EIA_OIL_FILE = RAW_DIR / "eia_monthly_crude_oil.csv"
EIA_GAS_FILE = RAW_DIR / "eia_monthly_natural_gas.csv"
NYSDEC_FILE = RAW_DIR / "nysdec_wells.csv"

SCHEMA_SQL = Path("sql/schema.sql")

LOG_LEVEL = "INFO"