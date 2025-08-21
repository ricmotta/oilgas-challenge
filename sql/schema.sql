-- Enable FK enforcement for this connection
PRAGMA foreign_keys = ON;

-- =========================
-- Dimension tables
-- =========================

CREATE TABLE IF NOT EXISTS dim_state (
    state_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code  TEXT NOT NULL UNIQUE,
    state_name  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_county (
    county_id    INTEGER PRIMARY KEY,
    state_id     INTEGER NOT NULL,
    county_name  TEXT NOT NULL,
    CONSTRAINT fk_county_state
        FOREIGN KEY (state_id) REFERENCES dim_state(state_id) ON DELETE RESTRICT,
    CONSTRAINT uq_county UNIQUE (state_id, county_name)
);

CREATE INDEX IF NOT EXISTS ix_county_state ON dim_county(state_id);

CREATE TABLE IF NOT EXISTS dim_operator (
    operator_id   INTEGER PRIMARY KEY,
    operator_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_well_status (
    status_id    INTEGER PRIMARY KEY,
    status_desc  TEXT
);

CREATE TABLE IF NOT EXISTS dim_well (
    well_id        INTEGER PRIMARY KEY,
    source_well_id TEXT,
    well_name      TEXT,
    state_id       INTEGER NOT NULL,
    county_id      INTEGER,
    operator_id    INTEGER,
    status_id      INTEGER,
    latitude       REAL,
    longitude      REAL,
    spud_date      TEXT,
    last_updated   TEXT,
    CONSTRAINT fk_well_state
        FOREIGN KEY (state_id) REFERENCES dim_state(state_id) ON DELETE RESTRICT,
    CONSTRAINT fk_well_county
        FOREIGN KEY (county_id) REFERENCES dim_county(county_id) ON DELETE SET NULL,
    CONSTRAINT fk_well_operator
        FOREIGN KEY (operator_id) REFERENCES dim_operator(operator_id) ON DELETE SET NULL,
    CONSTRAINT fk_well_status
        FOREIGN KEY (status_id) REFERENCES dim_well_status(status_id) ON DELETE SET NULL,
    CONSTRAINT uq_well_source UNIQUE (source_well_id),
    CONSTRAINT ck_lat CHECK (latitude  IS NULL OR (latitude  BETWEEN -90  AND 90)),
    CONSTRAINT ck_lon CHECK (longitude IS NULL OR (longitude BETWEEN -180 AND 180))
);

CREATE INDEX IF NOT EXISTS ix_well_state    ON dim_well(state_id);
CREATE INDEX IF NOT EXISTS ix_well_county   ON dim_well(county_id);
CREATE INDEX IF NOT EXISTS ix_well_operator ON dim_well(operator_id);

-- =========================
-- Fact table (state-level monthly production)
-- =========================

CREATE TABLE IF NOT EXISTS fact_state_production_monthly (
    prod_id       INTEGER PRIMARY KEY,
    state_id      INTEGER NOT NULL,
    period_month  TEXT    NOT NULL,  -- 'YYYY-MM-01' (first day of month)
    oil_bbl       REAL,              -- barrels in the month
    gas_mcf       REAL,              -- thousand cubic feet in the month
    source        TEXT,
    load_ts       TEXT    NOT NULL,
    CONSTRAINT fk_prod_state
        FOREIGN KEY (state_id) REFERENCES dim_state(state_id) ON DELETE RESTRICT,
    CONSTRAINT uq_state_month UNIQUE (state_id, period_month)
);

CREATE INDEX IF NOT EXISTS ix_prod_state_month
    ON fact_state_production_monthly(state_id, period_month);

-- =========================
-- Helper view for analytics
-- =========================

-- Wells per county
CREATE VIEW IF NOT EXISTS vw_wells_by_county AS
SELECT
    c.county_id,
    s.state_code,
    c.county_name,
    COUNT(w.well_id) AS well_count
FROM dim_well w
JOIN dim_county c ON w.county_id = c.county_id
JOIN dim_state  s ON c.state_id  = s.state_id
GROUP BY c.county_id, s.state_code, c.county_name;

-- =========================
-- Complete Seed of U.S. States
-- =========================
INSERT OR IGNORE INTO dim_state (state_code, state_name) VALUES
    ('AL', 'Alabama'),
    ('AK', 'Alaska'),
    ('AZ', 'Arizona'),
    ('AR', 'Arkansas'),
    ('CA', 'California'),
    ('CO', 'Colorado'),
    ('CT', 'Connecticut'),
    ('DE', 'Delaware'),
    ('DC', 'District of Columbia'),
    ('FL', 'Florida'),
    ('GA', 'Georgia'),
    ('HI', 'Hawaii'),
    ('ID', 'Idaho'),
    ('IL', 'Illinois'),
    ('IN', 'Indiana'),
    ('IA', 'Iowa'),
    ('KS', 'Kansas'),
    ('KY', 'Kentucky'),
    ('LA', 'Louisiana'),
    ('ME', 'Maine'),
    ('MD', 'Maryland'),
    ('MA', 'Massachusetts'),
    ('MI', 'Michigan'),
    ('MN', 'Minnesota'),
    ('MS', 'Mississippi'),
    ('MO', 'Missouri'),
    ('MT', 'Montana'),
    ('NE', 'Nebraska'),
    ('NV', 'Nevada'),
    ('NH', 'New Hampshire'),
    ('NJ', 'New Jersey'),
    ('NM', 'New Mexico'),
    ('NY', 'New York'),
    ('NC', 'North Carolina'),
    ('ND', 'North Dakota'),
    ('OH', 'Ohio'),
    ('OK', 'Oklahoma'),
    ('OR', 'Oregon'),
    ('PA', 'Pennsylvania'),
    ('RI', 'Rhode Island'),
    ('SC', 'South Carolina'),
    ('SD', 'South Dakota'),
    ('TN', 'Tennessee'),
    ('TX', 'Texas'),
    ('UT', 'Utah'),
    ('VT', 'Vermont'),
    ('VA', 'Virginia'),
    ('WA', 'Washington'),
    ('WV', 'West Virginia'),
    ('WI', 'Wisconsin'),
    ('WY', 'Wyoming');
