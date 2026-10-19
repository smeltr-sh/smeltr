-- Run once by the postgres image on an empty data directory, as `smeltr`, in the database `smeltr`.

-- The role the SQL source signs in as. It may read and nothing else; each test grants it SELECT on the tables it makes.
CREATE ROLE smeltr_reader LOGIN PASSWORD 'smeltr-live-1';

-- A table large enough that reading all of it is a full scan, to plan against: `indexed` has an index, `plain` none.
CREATE TABLE plan_big (id bigint PRIMARY KEY, indexed bigint NOT NULL, plain bigint NOT NULL);
INSERT INTO plan_big SELECT i, i, i FROM generate_series(1, 1000000) AS i;
CREATE INDEX plan_big_indexed ON plan_big (indexed);
ANALYZE plan_big;
GRANT SELECT ON plan_big TO smeltr_reader;
