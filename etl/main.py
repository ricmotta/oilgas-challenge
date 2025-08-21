import logging
from pathlib import Path
import argparse

from .config import (RAW_DIR, PROC_DIR, DB_PATH, EIA_OIL_FILE, EIA_GAS_FILE, NYSDEC_FILE, SCHEMA_SQL, LOG_LEVEL)
from .db.io import connect, apply_schema
from .loaders.eia import load_eia_pair
from .loaders.nysdec import load_nysdec
from .pipelines.load_all import load_dimensions_and_fact
from etl.geo.make_geojson import export_geojson_from_parquet

def ensure_dirs():
    PROC_DIR.mkdir(parents=True, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(description="Oil & Gas ETL runner")
    p.add_argument("--apply-schema", action="store_true", help="Apply sql/schema.sql before loading")
    p.add_argument("--make-geojson", action="store_true", help="Export wells.geojson after ETL")
    return p.parse_args()

def main():
    logging.basicConfig(level=getattr(logging, LOG_LEVEL), format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    ensure_dirs()

    with connect(DB_PATH) as conn:
        if args.apply_schema and SCHEMA_SQL.exists():
            logging.info("Applying schema.sql...")
            apply_schema(conn, SCHEMA_SQL)

        logging.info(f"Loading EIA from {EIA_OIL_FILE} and {EIA_GAS_FILE}")
        eia_df = load_eia_pair(EIA_OIL_FILE, EIA_GAS_FILE)

        logging.info(f"Loading NYSDEC from {NYSDEC_FILE}")
        wells_df = load_nysdec(NYSDEC_FILE)

        logging.info("Loading into SQLite...")
        load_dimensions_and_fact(conn, eia_df, wells_df)

    if args.make_geojson:
        logging.info("Exporting wells GeoJSON...")
        export_geojson_from_parquet(
            Path("data/processed/wells.parquet"),
            Path("data/processed/wells.geojson")
        )

    logging.info("ETL completed successfully.")

if __name__ == "__main__":
    main()
