CREATE TABLE metrics (
    region varchar NOT NULL,
    day date NOT NULL,
    value double precision
);

INSERT INTO metrics
SELECT r.region,
       DATE '2024-01-01' + g,
       g * 1.5
FROM (VALUES ('us-east'), ('us-west'), ('eu-west')) AS r(region)
CROSS JOIN generate_series(0, 99) AS g;