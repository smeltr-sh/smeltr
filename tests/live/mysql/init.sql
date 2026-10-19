-- Run once by the mysql and mariadb images on an empty data directory, as root, in the database `smeltr`. The image
-- makes `smeltr`, the database's owner, from its environment.

-- The user the SQL source signs in as. It may read and nothing else.
CREATE USER smeltr_reader IDENTIFIED BY 'smeltr-live-1';
GRANT SELECT ON smeltr.* TO smeltr_reader;

-- A table large enough that reading all of it is a full scan, to plan against: `indexed` has an index, `plain` none.
-- A million rows, from six digits crossed: neither server's way of counting to a million is the other's.
CREATE TABLE plan_big (id bigint PRIMARY KEY, indexed bigint NOT NULL, plain bigint NOT NULL);
CREATE TABLE digits (d int NOT NULL);
INSERT INTO digits VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
INSERT INTO plan_big
SELECT n, n, n FROM (
  SELECT 1 + a.d + 10 * b.d + 100 * c.d + 1000 * e.d + 10000 * f.d + 100000 * g.d AS n
  FROM digits a, digits b, digits c, digits e, digits f, digits g
) AS numbers;
DROP TABLE digits;
CREATE INDEX plan_big_indexed ON plan_big (indexed);
ANALYZE TABLE plan_big;
