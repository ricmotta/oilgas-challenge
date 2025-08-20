import pandas as pd
from pathlib import Path
from ..transforms.common import to_float_or_none, to_date_iso_or_none

def load_nysdec(path: Path) -> pd.DataFrame:
    """
    Return normalized well metadata:
    ['source_well_id','well_name','state_code','county_name','operator_name',
     'status_desc','latitude','longitude','spud_date','last_updated','coord_valid']
    """
    d = pd.read_csv(path, dtype=str)

    out = pd.DataFrame({
        "source_well_id": d["API_WellNo"].astype(str).str.strip(),
        "well_name": d.get("Well_Name", "").astype(str).str.strip(),
        "state_code": "NY",
        "county_name": d.get("County", "").astype(str).str.strip().str.title(),
        "operator_name": d.get("Company_name", "").astype(str).str.strip(),
        "status_desc": d.get("GeneralWellStatus", "").astype(str).str.strip(),
        "latitude": d.get("Surface_latitude", "").map(to_float_or_none),
        "longitude": d.get("Surface_Longitude", "").map(to_float_or_none),
        "spud_date": d.get("Date_Spudded", "").map(to_date_iso_or_none),
        "last_updated": d.get("Dt_Mod", "").map(to_date_iso_or_none),
    })

    out["coord_valid"] = (
        out["longitude"].between(-180, 180, inclusive="both")
        & out["latitude"].between(-90, 90, inclusive="both")
    )
    return out
