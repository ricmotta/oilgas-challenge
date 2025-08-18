@echo off
call .\.venv\Scripts\activate
python etl\pipeline.py --db sqlite:///data/oilgas.db --with-geojson
