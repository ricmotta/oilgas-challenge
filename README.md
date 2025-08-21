# Oil & Gas – Mini Data Platform (ETL + Analytics)

This project implements a compact data pipeline and analytics foundation using **public oil & gas datasets** (EIA + NYSDEC).
It covers schema design, ETL, basic geospatial export, and SQL queries required by the assessment.

---

## Contents

* [Project Structure](#project-structure)
* [Prerequisites](#prerequisites)
* [Setup (Environment & Dependencies)](#setup-environment--dependencies)
* [Data Placement](#data-placement)
* [Configuration](#configuration)
* [Run the ETL](#run-the-etl)
* [Optional: Export GeoJSON](#optional-export-geojson)
* [Outputs](#outputs)
* [Analytics (SQL)](#analytics-sql)
* [Assumptions](#assumptions)
* [Validation Checklist](#validation-checklist)
* [Troubleshooting](#troubleshooting)

---

## Project Structure

```
├── data/
│   ├── raw/                 # input files (downloaded manually)
│   └── processed/           # cleaned snapshots & exports
├── etl/
│   ├── config.py            # paths & settings
│   ├── main.py              # CLI orchestration
│   ├── db/
│   │   └── io.py            # DB helpers (SQLite)
│   ├── loaders/
│   │   ├── eia.py           # EIA (oil + gas) loader (monthly volumes)
│   │   └── nysdec.py        # NYSDEC wells loader (NY)
│   ├── pipelines/
│   │   └── load_all.py      # loads to SQLite and writes processed files
│   ├── transforms/
│   │   └── common.py        # small parsing/validation utils
│   └── geo/
│       └── make_geojson.py  # (optional) export wells to GeoJSON
├── sql/
│   ├── schema.sql           # DDL + state seed
│   └── analytics_queries.sql# required analytical queries
└── README.md
```

---

## Prerequisites

* **Python 3.10+** (tested)
* **pip**
* **SQLite** (bundled with Python; CLI optional)

---

## Setup (Environment & Dependencies)

```bash
# From the repo root
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

---

## Data Placement

The files are already available, but you can download them manually and place them under `data/raw/`:

1. **EIA – U.S. Monthly Crude Oil and Natural Gas Production**
   Site: [https://www.eia.gov/petroleum/production/#oil-tab](https://www.eia.gov/petroleum/production/#oil-tab)

   * Download the **Crude Oil** file → save as `data/raw/eia_monthly_crude_oil.csv`
   * Download the **Natural Gas (Gross Withdrawals)** file → save as `data/raw/eia_monthly_natural_gas.csv`

2. **NYSDEC – New York State Well Locations**
   Site: [https://dec.ny.gov/environmental-protection/oil-gas/wells-data-geographical-information/downloadable-data](https://dec.ny.gov/environmental-protection/oil-gas/wells-data-geographical-information/downloadable-data)

   * Download the **public well locations CSV** → save as `data/raw/nysdec_wells.csv`

---

## Configuration

Default paths are defined in `etl/config.py`:

* `EIA_OIL_FILE = data/raw/eia_monthly_crude_oil.csv`
* `EIA_GAS_FILE = data/raw/eia_monthly_natural_gas.csv`
* `NYSDEC_FILE = data/raw/nysdec_wells.csv`
* `DB_PATH = data/oilgas.db`
* `SCHEMA_SQL = sql/schema.sql`

No credentials are required (all data is public).

---

## Run the ETL

**First run (applies schema + loads data):**

```bash
python -m etl.main --apply-schema
```

What it does:

* Applies `sql/schema.sql` (creates tables + seeds all US states in `dim_state`)
* Loads EIA (oil + gas), computes **monthly volumes**:

  * `oil_bbl` (barrels per month)
  * `gas_mcf` (thousand cubic feet per month)
* Loads NYSDEC wells (NY), validates coordinates and basic fields
* Writes cleaned snapshots to `data/processed/`
* Upserts into SQLite:

  * Dimensions: `dim_state` (seeded), `dim_operator`, `dim_well_status`, `dim_county`, `dim_well`
  * Fact: `fact_state_production_monthly`

Re-runs are **idempotent** (safe to repeat).

---

## Optional: Export GeoJSON

Generate a **GeoJSON** with well points (WGS84 lon/lat) for mapping:

```bash
python -m etl.main --apply-schema --make-geojson
```

This will write `data/processed/wells.geojson`.

You can open the file at [https://geojson.io](https://geojson.io) for a quick visual check.

---

## Outputs

* **Database (SQLite)**: `data/oilgas.db`

  * Dimensions: `dim_state`, `dim_county`, `dim_operator`, `dim_well_status`, `dim_well`
  * Fact: `fact_state_production_monthly`
  * View for analysis if included in `schema.sql` (bonus)

* **Processed files** (`data/processed/`)

  * `production_monthly.parquet` – EIA after normalization (state × month with `oil_bbl`, `gas_mcf`)
  * `wells.parquet` – NYSDEC wells normalized (with `coord_valid`)
  * `wells_by_county.csv` – wells aggregated by county (delivered by ETL)
  * `wells.geojson` – point features for all wells with valid coordinates

---

## Analytics (SQL)

All required queries are in `sql/analytics_queries.sql`. They answer:

1. **WV total oil & gas over last 12 months**
2. **County with the highest number of wells**
3. **Average oil & gas per well** (simulate production ÷ well count by state)
4. **Year‑over‑Year production change**

How to run (examples):

```bash
# Windows (SQLite CLI installed)
sqlite3 data\oilgas.db ".read sql/analytics_queries.sql"
```

You can also run them from DBeaver/DataGrip by connecting to `data/oilgas.db`.

---

## Assumptions

* **EIA** files provide state‑level daily rates which are converted to **monthly volumes**:

  * `oil_bbl = kbpd * 1,000 * days_in_month`
  * `gas_mcf = MMcf/d * 1,000 * days_in_month`
* **NYSDEC** well dataset covers **New York only**; thus:

  * "Average production per well" is meaningful for states where we have well metadata (NY).
  * Queries are written to generalize across states; if you add WV/PA well registries, results will include them automatically.
* **State list** is seeded in `schema.sql` (all US states, including DC). Any non‑state labels in EIA (e.g., “U.S. Total”, “Federal Offshore …”) don’t join and are ignored.
* **Coordinates**: rows with missing/invalid lon/lat are flagged/filtered out for geospatial outputs.
* **SQLite** chosen for simplicity and reproducibility (no external services).

---

## Validation Checklist

* DB created:

  ```sql
  .schema dim_state
  SELECT COUNT(*) FROM dim_state;  -- expect ~51
  ```
* Fact populated:

  ```sql
  SELECT COUNT(*) FROM fact_state_production_monthly;
  SELECT * FROM fact_state_production_monthly ORDER BY period_month DESC LIMIT 5;
  ```
* Wells loaded:

  ```sql
  SELECT COUNT(*) FROM dim_well;
  SELECT county_name, COUNT(*) FROM dim_well w JOIN dim_county c USING(county_id) GROUP BY county_name ORDER BY 2 DESC LIMIT 5;
  ```
* Processed files exist:

  * `data/processed/production_monthly.parquet`
  * `data/processed/wells.parquet`
  * `data/processed/wells_by_county.csv`
  * (optional) `data/processed/wells.geojson`

---

## Troubleshooting

* **FileNotFoundError (raw files):** Check filenames/locations in `etl/config.py` and ensure files exist in `data/raw/`.
* **SQLite syntax error:** SQLite does **not** support `CREATE TABLE OR REPLACE`. Use `CREATE TABLE IF NOT EXISTS` or migration steps in `schema.sql`.
* **GeoJSON not created:** Ensure `geopandas` is installed and run with `--make-geojson`. Confirm `data/processed/wells.parquet` exists and contains valid coordinates.
* **Unmapped states:** With the state seed in `schema.sql`, any non‑state labels from EIA simply don’t join and are dropped; that’s expected.

---

**That’s it!**
You can now run the ETL, inspect results, and execute the analytics queries required by the assessment.
