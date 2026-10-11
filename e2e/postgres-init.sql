CREATE TABLE IF NOT EXISTS metrics (
    region varchar NOT NULL,
    day date NOT NULL,
    value double precision
);

TRUNCATE TABLE metrics;

INSERT INTO metrics
SELECT r.region,
       DATE '2024-01-01' + g,
       g * 1.5
FROM (VALUES ('us-east'), ('us-west'), ('eu-west')) AS r(region)
CROSS JOIN generate_series(0, 99) AS g;

CREATE TABLE IF NOT EXISTS regions (
    region varchar PRIMARY KEY,
    market varchar NOT NULL,
    manager varchar NOT NULL
);

INSERT INTO regions (region, market, manager)
VALUES
    ('us-east', 'North America', 'Avery'),
    ('us-west', 'North America', 'Jordan'),
    ('eu-west', 'Europe', 'Morgan')
ON CONFLICT (region) DO UPDATE
SET market = EXCLUDED.market,
    manager = EXCLUDED.manager;

CREATE TABLE IF NOT EXISTS daily_targets (
    region varchar NOT NULL REFERENCES regions(region),
    day date NOT NULL,
    target double precision NOT NULL,
    PRIMARY KEY (region, day)
);

INSERT INTO daily_targets (region, day, target)
SELECT r.region,
       DATE '2024-01-01' + g,
       (g * 1.5) + 10.0
FROM regions AS r
CROSS JOIN generate_series(0, 99) AS g
WHERE true
ON CONFLICT (region, day) DO UPDATE
SET target = EXCLUDED.target;

-- Deliberately NOT configured for caching in scent.properties (T16 opt-in check)
CREATE TABLE IF NOT EXISTS notes (
    id int PRIMARY KEY,
    payload varchar NOT NULL
);

INSERT INTO notes (id, payload)
VALUES (1, 'hello'), (2, 'world')
ON CONFLICT (id) DO UPDATE
SET payload = EXCLUDED.payload;
