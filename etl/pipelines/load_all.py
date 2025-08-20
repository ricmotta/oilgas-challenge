import pandas as pd
from datetime import datetime

from ..config import PROC_DIR

def load_dimensions_and_fact(conn, eia_df: pd.DataFrame, wells_df: pd.DataFrame) -> None:
    """
    Load EIA (oil/gas) and NYSDEC wells into SQLite.
    Expects:
      - eia_df columns: ['period_month','state_name','oil_bbl','gas_mcf']
      - wells_df columns: ['source_well_id','well_name','state_code','county_name',
                           'operator_name','status_desc','latitude','longitude',
                           'spud_date','last_updated','coord_valid']
    """
    cur = conn.cursor()

    # -----------------------
    # Seed states (idempotent)
    # -----------------------
    cur.executemany(
        "INSERT OR IGNORE INTO dim_state(state_code, state_name) VALUES (?,?)",
        [("WV", "West Virginia"), ("PA", "Pennsylvania"), ("NY", "New York")]
    )

    # -----------------------
    # FACT: state production
    # -----------------------
    # Map state_name -> state_id
    states = pd.read_sql("SELECT state_id, state_name FROM dim_state", conn)
    e = eia_df.merge(states, on="state_name", how="left")

    # Guard-rail: ensure all state_name matched a state_id
    if e["state_id"].isna().any():
        missing = e.loc[e["state_id"].isna(), "state_name"].unique().tolist()
        raise ValueError(f"Unmapped state_name in EIA data: {missing}")

    e["source"] = "EIA"
    e["load_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Persist processed snapshot
    e[["period_month", "state_name", "oil_bbl", "gas_mcf"]].to_parquet(
        PROC_DIR / "production_monthly.parquet", index=False
    )

    for r in e.itertuples(index=False):
        cur.execute(
            """
            INSERT INTO fact_state_production_monthly
              (state_id, period_month, oil_bbl, gas_mcf, source, load_ts)
            VALUES (?, date(?), ?, ?, ?, ?)
            ON CONFLICT(state_id, period_month) DO UPDATE SET
              oil_bbl = excluded.oil_bbl,
              gas_mcf = excluded.gas_mcf,
              source  = excluded.source,
              load_ts = excluded.load_ts
            """,
            (int(r.state_id), str(r.period_month), r.oil_bbl, r.gas_mcf, r.source, r.load_ts)
        )

    # ------------------------------------
    # DIMENSIONS from NYSDEC (NY only here)
    # ------------------------------------
    # Operators
    ops = wells_df["operator_name"].dropna().drop_duplicates().tolist()
    cur.executemany(
        "INSERT OR IGNORE INTO dim_operator(operator_name) VALUES (?)",
        [(o,) for o in ops if o]
    )

    # Status (use desc as code if no better code)
    sts = wells_df["status_desc"].dropna().drop_duplicates().tolist()
    cur.executemany(
        "INSERT OR IGNORE INTO dim_well_status(status_code, status_desc) VALUES (?,?)",
        [(s, s) for s in sts if s]
    )

    # Counties (NY)
    ny_row = cur.execute("SELECT state_id FROM dim_state WHERE state_code='NY'").fetchone()
    if ny_row is None:
        raise RuntimeError("NY state not found in dim_state.")
    ny_id = int(ny_row[0])

    counties = wells_df["county_name"].dropna().drop_duplicates().tolist()
    cur.executemany(
        "INSERT OR IGNORE INTO dim_county(state_id, county_name) VALUES (?,?)",
        [(ny_id, c) for c in counties if c]
    )

    # Build FK maps
    def map_from(sql: str, key: str, val: str):
        df = pd.read_sql(sql, conn)
        return dict(zip(df[key], df[val]))

    op_map = map_from(
        "SELECT operator_name, operator_id FROM dim_operator",
        "operator_name", "operator_id"
    )
    st_map = map_from(
        "SELECT status_desc, status_id FROM dim_well_status",
        "status_desc", "status_id"
    )
    c_map = map_from(
        f"SELECT county_name, county_id FROM dim_county WHERE state_id={ny_id}",
        "county_name", "county_id"
    )

    wells_df = wells_df.copy()
    wells_df["state_id"] = ny_id
    wells_df["operator_id"] = wells_df["operator_name"].map(op_map)
    wells_df["status_id"] = wells_df["status_desc"].map(st_map)
    wells_df["county_id"] = wells_df["county_name"].map(c_map)

    # Drop rows without a natural key (source_well_id)
    wells_df = wells_df[~wells_df["source_well_id"].isna() & (wells_df["source_well_id"].astype(str).str.len() > 0)]

    # Persist processed snapshot
    wells_df.to_parquet(PROC_DIR / "wells.parquet", index=False)

    # Insert wells (idempotent via UNIQUE(source_well_id))
    for r in wells_df.itertuples(index=False):
        cur.execute(
            """
            INSERT OR IGNORE INTO dim_well
              (source_well_id, well_name, state_id, county_id, operator_id, status_id,
               latitude, longitude, spud_date, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                r.source_well_id, r.well_name, r.state_id, r.county_id,
                r.operator_id, r.status_id, r.latitude, r.longitude,
                r.spud_date, r.last_updated
            )
        )

    conn.commit()
