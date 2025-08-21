import pandas as pd
from datetime import datetime

from ..config import PROC_DIR


def load_dimensions_and_fact(conn, eia_df: pd.DataFrame, wells_df: pd.DataFrame) -> None:
    """
    Load EIA (state-level monthly oil/gas) and NYSDEC wells (NY) into SQLite.

    Assumptions:
      - dim_state is fully seeded in schema.sql with all valid US states (code + name).
      - eia_df columns: ['period_month','state_name','oil_bbl','gas_mcf']
      - wells_df columns:
          ['source_well_id','well_name','state_code','county_name','operator_name','status_desc',
           'latitude','longitude','spud_date','last_updated','coord_valid']

    Behavior:
      - Joins EIA rows to dim_state by state_name; drops non-matching (aggregates like 'U.S. Total', offshore, etc.).
      - Upserts fact_state_production_monthly on (state_id, period_month).
      - Upserts dim_operator, dim_well_status, dim_county (NY only).
      - Inserts dim_well with UNIQUE(source_well_id).
      - Writes processed snapshots to data/processed/.
    """
    cur = conn.cursor()

    # -------------------------------------------------------------------------
    # 1) FACT: state-level monthly production (idempotent upsert)
    # -------------------------------------------------------------------------
    # Join EIA with seeded dim_state
    states = pd.read_sql("SELECT state_id, state_name FROM dim_state", conn)
    e = eia_df.merge(states, on="state_name", how="left")

    # Drop any non-matching rows (aggregates or unexpected labels)
    if e["state_id"].isna().any():
        unmapped = (
            e.loc[e["state_id"].isna(), "state_name"]
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        if unmapped:
            # Log to console for awareness; not raising to keep pipeline robust.
            print(f"[WARN] Dropping non-state/unknown labels from EIA: {unmapped}")
        e = e[e["state_id"].notna()].copy()

    e["source"] = "EIA"
    e["load_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Persist processed snapshot (post-join, only valid states)
    e[["period_month", "state_name", "oil_bbl", "gas_mcf"]].to_parquet(
        PROC_DIR / "production_monthly.parquet", index=False
    )

    # Upsert into fact
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

    # -------------------------------------------------------------------------
    # 2) DIMENSIONS from NYSDEC (NY only here)
    # -------------------------------------------------------------------------
    # Operators
    ops = wells_df["operator_name"].dropna().drop_duplicates().tolist()
    if ops:
        cur.executemany(
            "INSERT OR IGNORE INTO dim_operator(operator_name) VALUES (?)",
            [(o,) for o in ops if o]
        )

    # Status (use description as code when no better code is provided)
    sts = wells_df["status_desc"].dropna().drop_duplicates().tolist()
    if sts:
        cur.executemany(
            "INSERT OR IGNORE INTO dim_well_status(status_desc) VALUES (?)",
            [(s,) for s in sts if s]
        )

    # Counties (NY)
    ny_row = cur.execute("SELECT state_id FROM dim_state WHERE state_code='NY'").fetchone()
    if ny_row is None:
        raise RuntimeError("NY state not found in dim_state (schema seed expected to have inserted it).")
    ny_id = int(ny_row[0])

    counties = wells_df["county_name"].dropna().drop_duplicates().tolist()
    if counties:
        cur.executemany(
            "INSERT OR IGNORE INTO dim_county(state_id, county_name) VALUES (?,?)",
            [(ny_id, c) for c in counties if c]
        )

    # -------------------------------------------------------------------------
    # 3) FK maps and well inserts
    # -------------------------------------------------------------------------
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

    wells = wells_df.copy()
    wells["state_id"]    = ny_id
    wells["operator_id"] = wells["operator_name"].map(op_map)
    wells["status_id"]   = wells["status_desc"].map(st_map)
    wells["county_id"]   = wells["county_name"].map(c_map)

    # Drop rows without natural key (source_well_id)
    wells = wells[
        ~wells["source_well_id"].isna()
        & (wells["source_well_id"].astype(str).str.len() > 0)
    ].copy()

    # Persist processed snapshot
    wells.to_parquet(PROC_DIR / "wells.parquet", index=False)

    # Insert wells (idempotent by UNIQUE(source_well_id))
    for r in wells.itertuples(index=False):
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

    # -------------------------------------------------------------------------
    # 4) Deliverable: wells by county (CSV)
    # -------------------------------------------------------------------------
    wbc = (
        wells[wells["coord_valid"]]
        .groupby("county_name", dropna=True)
        .size()
        .reset_index(name="well_count")
        .sort_values("well_count", ascending=False)
    )
    wbc.to_csv(PROC_DIR / "wells_by_county.csv", index=False)

    conn.commit()
