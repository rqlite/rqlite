PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id integer not null primary key, name text);
INSERT INTO foo VALUES(1,'fiona');
INSERT INTO foo VALUES(2,'fiona');
INSERT INTO foo VALUES(3,'fiona');
COMMIT;
