import pandas as pd
import geopandas as gpd
from pathlib import Path

def export_geojson_from_parquet(parquet_path: Path, out_path: Path):
    df = pd.read_parquet(parquet_path)

    # Filter only valid coordinates
    df = df[
        df["latitude"].notna() & df["longitude"].notna()
        & df["latitude"].between(-90, 90)
        & df["longitude"].between(-180, 180)
    ].copy()

    # Create GeoDataFrame with CRS WGS84
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326"
    )

    # Export as GeoJSON
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_path, driver="GeoJSON")
