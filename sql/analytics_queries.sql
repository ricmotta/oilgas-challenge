-- 1) Total oil & gas from West Virginia (last 12 months)
WITH wv AS (
  SELECT 
  	date(p.period_month) AS period_dt,
    p.oil_bbl, 
    p.gas_mcf
  FROM fact_state_production_monthly p
  INNER JOIN dim_state s 
    ON s.state_id = p.state_id
  WHERE s.state_code = 'WV'
),
maxp AS (
  SELECT MAX(period_dt) AS max_dt FROM wv
),
last12 AS (
  SELECT 
  	wv.period_dt,
  	wv.oil_bbl,
  	wv.gas_mcf
  FROM wv
  CROSS JOIN maxp
  WHERE wv.period_dt >= date(maxp.max_dt, '-11 months')
)
SELECT
  'WV total last 12 months' AS metric,
  SUM(oil_bbl) AS total_oil_bbl,
  SUM(gas_mcf) AS total_gas_mcf
FROM last12;

-- 2) Which county had the highest number of wells? (using NYSDEC data)
WITH well_counts AS (
  SELECT 
    s.state_code,
    c.county_name,
    COUNT(w.well_id) AS well_count
  FROM dim_well AS w
  INNER JOIN dim_county AS c
    ON c.county_id = w.county_id
  INNER JOIN dim_state AS s
    ON s.state_id = c.state_id
  WHERE w.county_id IS NOT NULL
  GROUP BY s.state_code, c.county_name
)
SELECT
  state_code,
  county_name,
  well_count
FROM well_counts
ORDER BY well_count DESC, state_code ASC, county_name ASC
LIMIT 1;

-- 3) Average oil & gas production per well

WITH params AS (
  SELECT 'NY' AS target_state_code
),
state_monthly AS (
  SELECT 
    p.period_month,
    p.oil_bbl,
    p.gas_mcf,
    DATE(p.period_month) AS period_dt
  FROM fact_state_production_monthly AS p
  INNER JOIN dim_state AS s
    ON s.state_id = p.state_id
  INNER JOIN params AS prm
    ON s.state_code = prm.target_state_code
),
maxp AS (
  SELECT MAX(period_dt) AS max_dt
  FROM state_monthly
),
last12 AS (
  SELECT sm.period_dt, sm.oil_bbl, sm.gas_mcf
  FROM state_monthly AS sm
  CROSS JOIN maxp
  WHERE sm.period_dt >= DATE(maxp.max_dt, '-11 months')
),
state_well_count AS (
  SELECT
    COUNT(w.well_id) AS wells_in_state
  FROM dim_well AS w
  INNER JOIN dim_county AS c
    ON c.county_id = w.county_id
  INNER JOIN dim_state AS s
    ON s.state_id = c.state_id
  INNER JOIN params AS prm
    ON s.state_code = prm.target_state_code
  -- (optional) consider only valid coordinates:
  -- WHERE w.latitude IS NOT NULL AND w.longitude IS NOT NULL
)
SELECT
  (SELECT target_state_code FROM params)            AS state_code,
  SUM(l12.oil_bbl)                                  AS total_oil_bbl_12mo,
  SUM(l12.gas_mcf)                                  AS total_gas_mcf_12mo, -- EIA has no gas data for NY
  swc.wells_in_state                                AS wells_in_state,
  -- average production per well last 12 months (total / #wells)
  CAST(SUM(l12.oil_bbl) AS REAL) / NULLIF(swc.wells_in_state, 0) AS oil_bbl_per_well_12mo,
  CAST(SUM(l12.gas_mcf) AS REAL) / NULLIF(swc.wells_in_state, 0) AS gas_mcf_per_well_12mo
FROM last12 AS l12
CROSS JOIN state_well_count AS swc;

-- 4) Year-over-Year production change (by state, annual totals)
WITH base AS (
  SELECT
    s.state_code,
    CAST(strftime('%Y', p.period_month) AS INTEGER) AS year,
    p.oil_bbl,
    p.gas_mcf
  FROM fact_state_production_monthly AS p
  INNER JOIN dim_state AS s
    ON s.state_id = p.state_id
),
annual AS (
  SELECT
    state_code,
    year,
    SUM(oil_bbl) AS oil_bbl_year,
    SUM(gas_mcf) AS gas_mcf_year
  FROM base
  GROUP BY state_code, year
),
yoy AS (
  SELECT
    state_code,
    year,
    oil_bbl_year,
    gas_mcf_year,
    LAG(oil_bbl_year) OVER (PARTITION BY state_code ORDER BY year) AS prev_oil_bbl_year,
    LAG(gas_mcf_year) OVER (PARTITION BY state_code ORDER BY year) AS prev_gas_mcf_year
  FROM annual
)
SELECT
  state_code,
  year,
  oil_bbl_year,
  prev_oil_bbl_year,
  (oil_bbl_year - prev_oil_bbl_year)                       AS oil_bbl_yoy_delta,
  CASE
    WHEN prev_oil_bbl_year IS NULL OR prev_oil_bbl_year = 0 THEN NULL
    ELSE 100.0 * (oil_bbl_year - prev_oil_bbl_year) / prev_oil_bbl_year
  END AS oil_bbl_yoy_pct,
  gas_mcf_year,
  prev_gas_mcf_year,
  (gas_mcf_year - prev_gas_mcf_year)                       AS gas_mcf_yoy_delta,
  CASE
    WHEN prev_gas_mcf_year IS NULL OR prev_gas_mcf_year = 0 THEN NULL
    ELSE 100.0 * (gas_mcf_year - prev_gas_mcf_year) / prev_gas_mcf_year
  END AS gas_mcf_yoy_pct
FROM yoy
ORDER BY state_code, year;
