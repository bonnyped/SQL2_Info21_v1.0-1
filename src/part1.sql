--------------------------------------------
----------- CREATE DATABASE ----------------
--------------------------------------------

-- CREATE DATABASE model_s21;

--------------------------------------------
-------------- DROP ALL --------------------
--------------------------------------------
DROP TABLE IF EXISTS Friends  CASCADE ;
DROP TABLE IF EXISTS TransferredPoints;
DROP TABLE IF EXISTS Recommendations;
DROP TABLE IF EXISTS TimeTracking;
DROP TABLE IF EXISTS p2p;
DROP TABLE IF EXISTS checks CASCADE ;
DROP TABLE IF EXISTS Verter;
DROP TABLE IF EXISTS xp;
DROP TABLE IF EXISTS tasks;
DROP SEQUENCE IF EXISTS id_for_checks CASCADE ;
DROP SEQUENCE IF EXISTS id_for_p2p CASCADE;
DROP SEQUENCE IF EXISTS id_for_verter CASCADE;
DROP SEQUENCE IF EXISTS id_for_xp CASCADE;
DROP TYPE IF EXISTS check_state CASCADE;
DROP TABLE IF EXISTS Peers;

DROP FUNCTION IF EXISTS fnc_p2p_success CASCADE;
DROP FUNCTION IF EXISTS fnc_p2p_or_verter_success CASCADE;
DROP FUNCTION IF EXISTS fnc_xp_lq_max CASCADE;

DROP PROCEDURE IF EXISTS  proc_export_all_tables_to_csv;
DROP PROCEDURE IF EXISTS  proc_export_table_to_csv;
DROP PROCEDURE IF EXISTS  proc_import_from_csv;

--------------------------------------------
----- PROCEDURES EXPORT & IMPORT -----------
--------------------------------------------
CREATE OR REPLACE PROCEDURE proc_export_all_tables_to_csv(path TEXT, "delim" CHAR DEFAULT ',')
    LANGUAGE plpgsql AS
$EXPORT_ALL_TABLES$
DECLARE
    tables    RECORD;
    statement TEXT;
BEGIN
    FOR tables IN
        SELECT ("current_schema"() || '.' || table_name) AS table_with_schema
        FROM (SELECT DISTINCT table_name
              from information_schema.columns
              where table_schema = "current_schema"())
        LOOP
            statement := format('COPY %s to ''%s/%s.csv'' DELIMITER ''%s'' CSV HEADER;', tables.table_with_schema, path, tables.table_with_schema,  delim);
            EXECUTE statement;
        END LOOP;
END;
$EXPORT_ALL_TABLES$;

--------------------------------------------
CREATE OR REPLACE PROCEDURE proc_export_table_to_csv(table_name_s21 TEXT, path_to_file TEXT, "delim" CHAR DEFAULT ',')
    LANGUAGE plpgsql AS
$EXPORT_TABLE$
    DECLARE
        schema_name VARCHAR DEFAULT "current_schema"();
        statement TEXT;
BEGIN
    statement := format('COPY %s.%s TO ''%s/%s.csv'' DELIMITER ''%s'' CSV HEADER;',schema_name, table_name_s21 , path_to_file, table_name_s21, "delim");
        EXECUTE statement;
END;
   $EXPORT_TABLE$;

--------------------------------------------

CREATE OR REPLACE PROCEDURE proc_import_from_csv(in s21_table_name VARCHAR, in path_to_file text, delimiter_csv CHAR DEFAULT ',' )
LANGUAGE plpgsql AS
$IMPORT$
DECLARE
    c_schema_name VARCHAR DEFAULT "current_schema"();
BEGIN
IF (
        EXISTS (
            SELECT table_catalog, table_schema, table_name, table_type
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = lower (s21_table_name) AND c_schema_name = table_schema AND lower(table_type) = 'base table'
        )
    ) THEN
    EXECUTE format('COPY  %s FROM ''%s''  DELIMITER  ''%s''  CSV HEADER;',s21_table_name,path_to_file, delimiter_csv);
    ELSE RAISE EXCEPTION 'The table does not exist: %',
    s21_table_name;
    END IF;
END;
$IMPORT$;

--------------------------------------------
---------- FUNCTIONS CHECK -----------------
--------------------------------------------
CREATE TYPE check_state AS ENUM ('start', 'success', 'fail');

--------------------------------------------
CREATE OR REPLACE FUNCTION fnc_p2p_success("checks_id" BIGINT)
RETURNS BOOLEAN AS
$$
BEGIN
IF 'success' IN (SELECT "state" FROM p2p WHERE "check" = "checks_id")
THEN RETURN true;
ELSE RETURN false;
END IF;
END;
$$ LANGUAGE PLpgSQL;

--------------------------------------------
CREATE OR REPLACE FUNCTION fnc_p2p_or_verter_success(id_for_check BIGINT) RETURNS boolean
    LANGUAGE plpgsql AS
$$
DECLARE
    state_of_check_in_verter check_state := (SELECT state
                                             FROM Verter v
                                             WHERE v."check" = id_for_check
                                               AND v.state != 'start');
    state_of_check_in_p2p    check_state := (SELECT state
                                             FROM p2p p
                                             WHERE p."check" = id_for_check
                                               AND p.state != 'start');
BEGIN
    IF state_of_check_in_p2p = 'fail' THEN
        RETURN FALSE;
    ELSE
        IF state_of_check_in_verter = 'fail' THEN
            RETURN FALSE;
        END IF;
    END IF;
    RETURN TRUE;
END;
$$;

--------------------------------------------
CREATE OR REPLACE FUNCTION fnc_xp_lq_max("check" BIGINT, xp_amount SMALLINT) RETURNS boolean
    LANGUAGE plpgsql AS
    $$
    BEGIN
        IF (SELECT max_xp
            FROM tasks t,
                 checks ch
            WHERE t.title = ch.task
              AND "check" = ch.id) >= xp_amount THEN
            RETURN TRUE;
            ELSE
            RETURN FALSE;
        END IF;
        RETURN TRUE;
        END;
    $$;

--------------------------------------------
------------- CREATE ALL -------------------
--------------------------------------------
CREATE SEQUENCE id_for_p2p;
CREATE SEQUENCE id_for_checks;
CREATE SEQUENCE id_for_verter;
CREATE SEQUENCE id_for_xp;

--------------------------------------------
CREATE TABLE Peers (
    Nickname VARCHAR NOT NULL PRIMARY KEY UNIQUE,
    Birthday DATE NOT NULL default current_date::DATE
);

CREATE TABLE IF NOT EXISTS Friends (
    ID BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    Peer1 VARCHAR NOT NULL,
    Peer2 VARCHAR NOT NULL,
    FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
    FOREIGN KEY (Peer2) REFERENCES Peers (Nickname)
);

CREATE TABLE IF NOT EXISTS TransferredPoints (
    ID BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    -- generation??
    CheckingPeer VARCHAR NOT NULL,
    CheckedPeer VARCHAR NOT NULL,
    PointsAmount BIGINT DEFAULT 0,
    FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
    FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname)
);

CREATE TABLE IF NOT EXISTS TimeTracking (
    ID BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    Peer VARCHAR NOT NULL,
    "Date" DATE DEFAULT CURRENT_TIMESTAMP::DATE,
    "Time" TIME DEFAULT CURRENT_TIMESTAMP::TIME,
    "State" SMALLINT DEFAULT 1 check ("State" IN (1, 2)),
    FOREIGN KEY (Peer) REFERENCES Peers (Nickname)
);

CREATE TABLE IF NOT EXISTS Recommendations (
    ID BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    Peer VARCHAR NOT NULL,
    RecommendedPeer VARCHAR NOT NULL,
    FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname)
);

CREATE TABLE IF NOT EXISTS Tasks (
    "title" varchar PRIMARY KEY,
    parent_task varchar,
    max_xp smallint DEFAULT 0
);

CREATE TABLE IF NOT EXISTS Checks (
    id bigint PRIMARY KEY DEFAULT nextval('id_for_checks'),
    peer varchar NOT NULL,
    task varchar NOT NULL,
    "date" date NOT NULL DEFAULT current_date,
    FOREIGN KEY (peer) references Peers(nickname),
    FOREIGN KEY (task) references tasks(title)
);

CREATE TABLE IF NOT EXISTS p2p (
    id bigint PRIMARY KEY DEFAULT nextval('id_for_p2p'),
    "check" bigint NOT NULL,
    CheckingPeer varchar NOT NULL,
    "state" check_state DEFAULT 'start'::check_state,
    "time" time NOT NULL DEFAULT current_time,
    FOREIGN KEY ("check") references checks(id),
    FOREIGN KEY (CheckingPeer) references Peers(nickname)
);

CREATE TABLE IF NOT EXISTS Verter (
  id bigint PRIMARY KEY DEFAULT nextval('id_for_verter'),
  "check" bigint NOT NULL,
  "state" check_state DEFAULT 'start'::check_state,
  "time" time DEFAULT current_time,
  FOREIGN KEY ("check") references checks(id)
);

CREATE TABLE IF NOT EXISTS Xp (
    id        bigint PRIMARY KEY DEFAULT nextval('id_for_xp'),
    "check"   bigint NOT NULL check ( fnc_p2p_or_verter_success("check") ),
    XP_amount smallint NOT NULL check ( fnc_xp_lq_max("check", XP_amount) ),
    FOREIGN KEY ("check") references checks(id));

--------------------------------------------
------------ INSERT data -------------------
--------------------------------------------
INSERT INTO Peers (Nickname, Birthday)
VALUES ('kennethgraham', '1999-02-23'),
    ('nancywilson', '1978-06-08'),
    ('troybrown', '1964-11-03'),
    ('laurenwood', '2004-02-28'),
    ('nancymartinez', '1995-08-06'),
    ('pamelawalker', '1992-12-13'),
    ('lorigarrett', '1970-09-30'),
    ('josepayne', '1988-01-07'),
    ('frankray', '2005-05-25'),
    ('lloydmartin', '1999-12-06');

INSERT INTO Friends (Peer1, Peer2)
VALUES ('kennethgraham', 'nancymartinez'),
    ('kennethgraham', 'laurenwood'),
    ('troybrown', 'nancywilson'),
    ('nancywilson', 'laurenwood'),
    ('laurenwood', 'nancymartinez'),
    ('josepayne', 'lloydmartin');

INSERT INTO Recommendations (Peer, RecommendedPeer)
VALUES ('laurenwood', 'nancymartinez'),
        ('kennethgraham', 'laurenwood'),
        ('laurenwood', 'nancywilson'),
        ('nancywilson', 'laurenwood'),
        ('laurenwood', 'troybrown'),
        ('kennethgraham', 'josepayne'),
        ('lloydmartin', 'josepayne'),
        ('lloydmartin', 'frankray'),
        ('nancymartinez', 'nancywilson'),
        ('laurenwood', 'josepayne');
INSERT INTO TimeTracking (id, Peer, "Date", "Time", "State")
VALUES (1,'kennethgraham','2022-12-22','11:01:13',1),
        (2,'nancywilson','2022-12-22','15:05:33',1),
        (3,'nancywilson','2022-12-22','9:55:44',2),
        (4,'kennethgraham','2022-12-22','21:40:14',2),
        (5,'kennethgraham','2022-12-23','7:00:55',1),
        (6,'kennethgraham','2022-12-23','9:55:44',2),
        (7,'laurenwood','2022-12-23','10:14:00',1),
        (8,'laurenwood','2022-12-23','14:02:00',2),
        (9,'laurenwood','2022-12-23','14:19:00',1),
        (10,'laurenwood','2022-12-23','19:43:00',2),
        (11,'nancywilson','2022-12-23','22:04:00',1),
        (12,'lloydmartin','2022-12-24','9:43:00',1),
        (13,'nancywilson','2022-12-23','13:04:00',2),
        (14,'lloydmartin','2022-12-24','19:43:00',2),
        (15,'kennethgraham','2022-12-25','10:54:23',1),
        (16,'lloydmartin','2022-12-25','10:59:42',1),
        (17,'nancywilson','2022-12-25','12:13:14',1),
        (18,'kennethgraham','2022-12-25','13:22:49',2),
        (19,'kennethgraham','2022-12-25','13:50:32',1),
        (20,'laurenwood','2022-12-25','16:44:33',1),
        (21,'lloydmartin','2022-12-25','18:31:00',2),
        (22,'nancywilson','2022-12-25','18:47:13',2),
        (23,'kennethgraham','2022-12-25','19:55:33',2),
        (24,'kennethgraham','2022-12-25','21:01:00',2),
        (25,'laurenwood','2022-12-25','23:49:14',2);

INSERT INTO tasks(title, parent_task, max_xp)
VALUES ('C6_s21_matrix', NULL ,200),
        ('C7_SmartCalc_v1.0','C6_s21_matrix',500),
        ('C8_3DViewer_v1.0','C7_SmartCalc_v1.0',750),
        ('CPP1_s21_matrix+','C8_3DViewer_v1.0',300),
        ('CPP2_s21_containers','CPP1_s21_matrix+',350),
        ('CPP3_SmartCalc_v2.0','CPP2_s21_containers',600),
        ('A1_MAZE','CPP3_SmartCalc_v2.0',300),
        ('A2_SimpleNavigator v1.0','A1_MAZE',400),
        ('A3_Parallels','A2_SimpleNavigator v1.0',300),
        ('DO1_Linux','C6_s21_matrix',300),
        ('DO2_Linux_Network','DO1_Linux',250),
        ('DO3_LinuxMonitoring_v1.0','DO2_Linux_Network',350);

INSERT INTO checks (peer, task, date)
VALUES ('frankray','C6_s21_matrix','2022-11-01'),
        ('frankray','C7_SmartCalc_v1.0','2022-11-20'),
        ('frankray','C8_3DViewer_v1.0','2022-12-05'),
        ('kennethgraham','C6_s21_matrix','2022-11-05'),
        ('kennethgraham','C7_SmartCalc_v1.0','2022-11-18'),
        ('kennethgraham','C8_3DViewer_v1.0','2022-12-05'),
        ('laurenwood','C6_s21_matrix','2022-12-05'),
        ('laurenwood','C7_SmartCalc_v1.0','2022-12-27'),
        ('laurenwood','C8_3DViewer_v1.0','2023-01-01'),
        ('laurenwood','C8_3DViewer_v1.0','2023-01-01'),
        ('nancywilson','C6_s21_matrix','2022-11-20'),
        ('nancywilson','C7_SmartCalc_v1.0','2022-12-01'),
        ('nancywilson','C8_3DViewer_v1.0','2022-12-18'),
        ('pamelawalker','C6_s21_matrix','2022-11-02'),
        ('pamelawalker','C7_SmartCalc_v1.0','2022-11-25'),
        ('pamelawalker','C8_3DViewer_v1.0','2022-12-10'),
        ('troybrown','C6_s21_matrix','2022-10-30'),
        ('troybrown','C7_SmartCalc_v1.0','2022-11-11'),
        ('troybrown','C8_3DViewer_v1.0','2022-11-30'),
        ('kennethgraham','CPP1_s21_matrix+','2023-01-01'),
        ('kennethgraham','CPP1_s21_matrix+','2023-01-04'),
        ('nancywilson','CPP1_s21_matrix+','2023-01-02'),
        ('troybrown','CPP1_s21_matrix+','2023-01-10'),
        ('kennethgraham','CPP2_s21_containers','2023-01-20'),
        ('kennethgraham','CPP3_SmartCalc_v2.0','2023-01-30'),
        ('frankray','CPP1_s21_matrix+','2023-01-31'),
        ('pamelawalker','CPP1_s21_matrix+','2023-01-31'),
        ('pamelawalker','CPP2_s21_containers','2023-02-11'),
        ('kennethgraham','CPP2_s21_containers','2023-02-23'),
        ('pamelawalker','CPP3_SmartCalc_v2.0','2023-02-28'),
        ('frankray','CPP3_SmartCalc_v2.0','2023-02-28'),
        ('laurenwood','CPP1_s21_matrix+','2023-02-28'),
        ('laurenwood','CPP1_s21_matrix+','2023-02-28'),
        ('nancywilson','CPP2_s21_containers','2023-02-28');

INSERT INTO p2p ("check", checkingpeer, "state", "time")
VALUES (1,'nancymartinez','start','10:05:00'),
(1,'nancymartinez','success','10:45:00'),
(2,'kennethgraham','start','11:55:43'),
(2,'kennethgraham','success','12:23:33'),
(3,'laurenwood','start','14:01:45'),
(3,'laurenwood','success','14:32:04'),
(4,'frankray','start','20:03:01'),
(4,'frankray','success','21:10:01'),
(5,'pamelawalker','start','23:00:00'),
(5,'pamelawalker','success','23:25:34'),
(6,'troybrown','start','21:15:00'),
(6,'troybrown','success','21:47:23'),
(7,'kennethgraham','start','13:30:00'),
(7,'kennethgraham','success','13:47:23'),
(8,'frankray','start','16:15:00'),
(8,'frankray','success','16:53:53'),
(9,'nancywilson','start','17:00:00'),
(9,'nancywilson','success','17:05:01'),
(10,'nancymartinez','start','21:30:00'),
(10,'nancymartinez','success','22:14:51'),
(11,'nancymartinez','start','16:00:00'),
(11,'nancymartinez','success','16:43:18'),
(12,'pamelawalker','start','19:00:00'),
(12,'pamelawalker','success','19:44:43'),
(13,'troybrown','start','14:15:00'),
(13,'troybrown','success','14:47:12'),
(14,'frankray','start','12:30:00'),
(14,'frankray','success','13:01:18'),
(15,'kennethgraham','start','9:00:00'),
(15,'kennethgraham','success','14:30:33'),
(16,'laurenwood','start','12:45:00'),
(16,'laurenwood','success','13:23:54'),
(17,'nancywilson','start','22:00:00'),
(17,'nancywilson','success','22:43:22'),
(18,'kennethgraham','start','0:00:15'),
(18,'kennethgraham','success','1:07:18'),
(19,'laurenwood','start','13:30:00'),
(19,'laurenwood','success','13:59:49'),
(20,'nancymartinez','start','19:15:14'),
(20,'nancymartinez','fail','19:45:14'),
(21,'nancywilson','start','20:45:14'),
(21,'nancywilson','success','21:15:14'),
(22,'kennethgraham','start','6:45:14'),
(22,'kennethgraham','success','7:10:14'),
(23,'laurenwood','start','2:25:14'),
(23,'laurenwood','success','2:58:14'),
(24,'nancywilson','start','22:48:47'),
(24,'nancywilson','success','23:15:47'),
(25,'nancymartinez','start','0:20:19'),
(25,'nancymartinez','success','1:10:19'),
(26,'pamelawalker','start','22:45:13'),
(26,'pamelawalker','success','23:20:43'),
(27,'frankray','start','12:45:23'),
(27,'frankray','success','13:29:10'),
(28,'frankray','start','9:30:24'),
(28,'frankray','success','10:15:01'),
(29,'frankray','start','16:17:41'),
(29,'frankray','success','16:50:41'),
(30,'kennethgraham','start','11:30:00'),
(30,'kennethgraham','success','11:55:33'),
(31,'troybrown','start','12:00:00'),
(31,'troybrown','success','12:31:10'),
(32,'frankray','start','13:58:21'),
(32,'frankray','fail','14:32:56'),
(33,'nancymartinez','start','22:05:05'),
(33,'nancymartinez','success','22:55:05'),
(34,'kennethgraham','start','22:15:00'),
(34,'kennethgraham','success','22:45:00');


INSERT INTO TransferredPoints (CheckingPeer,checkedpeer, PointsAmount)
    SELECT p2p.CheckingPeer, Checks.Peer, count(*)
    FROM p2p JOIN checks ON p2p.Check = Checks.Id
    WHERE p2p."state" != 'start'
    GROUP BY  p2p.CheckingPeer, checks.Peer;


INSERT INTO verter (id, "check", "state", "time")
VALUES (1,1,'start','10:45:03'),
        (2,1,'success','10:45:45'),
        (3,2,'start','12:23:33'),
        (4,2,'success','12:23:53'),
        (5,3,'start','14:32:04'),
        (6,3,'success','14:32:44'),
        (7,4,'start','20:03:01'),
        (8,4,'success','20:03:31'),
        (9,5,'start','23:25:34'),
        (10,5,'success','23:25:59'),
        (11,6,'start','21:47:23'),
        (12,6,'success','21:47:45'),
        (13,7,'start','13:47:23'),
        (14,7,'success','13:47:55'),
        (15,8,'start','16:53:53'),
        (16,8,'success','16:54:13'),
        (17,9,'start','17:05:01'),
        (18,9,'fail','17:05:11'),
        (19,10,'start','22:14:51'),
        (20,10,'success','22:15:21'),
        (21,11,'start','16:43:18'),
        (22,11,'success','16:43:51'),
        (23,12,'start','19:44:43'),
        (24,12,'success','19:45:21'),
        (25,13,'start','14:47:12'),
        (26,13,'success','14:47:39'),
        (27,14,'start','13:01:18'),
        (28,14,'success','13:01:33'),
        (29,15,'start','14:30:33'),
        (30,15,'success','14:31:01'),
        (31,16,'start','13:23:54'),
        (32,16,'success','13:24:33'),
        (33,17,'start','22:43:22'),
        (34,17,'success','22:43:51'),
        (35,18,'start','1:07:18'),
        (36,18,'success','1:07:29'),
        (37,19,'start','13:59:49'),
        (38,19,'success','14:00:24'),
        (39,21,'start','21:15:15'),
        (40,21,'success','21:16:04'),
        (41,22,'start','7:10:25'),
        (42,22,'success','7:10:44'),
        (43,23,'start','2:58:25'),
        (44,23,'success','2:59:09'),
        (45,25,'start','1:10:49'),
        (46,25,'success','1:11:19'),
        (47,26,'start','23:20:48'),
        (48,26,'success','23:20:59'),
        (49,27,'success','13:29:15'),
        (50,27,'start','13:29:45'),
        (51,30,'start','11:55:33'),
        (52,30,'success','11:55:57'),
        (53,31,'start','12:31:11'),
        (54,31,'success','12:31:55'),
        (55,33,'start','22:55:12'),
        (56,33,'success','22:55:33');

INSERT INTO xp ("check", XP_amount)
VALUES (1,189),
        (2,360),
        (3,733),
        (4,200),
        (5,455),
        (6,599),
        (7,199),
        (8,433),
        (10,750),
        (11,200),
        (12,450),
        (13,743),
        (14,200),
        (15,500),
        (16,750),
        (17,200),
        (18,399),
        (19,750),
        (21,299),
        (22,300),
        (23,255),
        (24,200),
        (25,600),
        (26,300),
        (27,277),
        (28,345),
        (29,350),
        (30,580),
        (31,600),
        (33,289),
        (34,330);


-- ************************************** --
--------- tests export/import  -------------
--------------------------------------------

-- CALL export_all_tables_to_csv('YOUR_ABSOLUTLY_PATH_TO_FILE/csv/', ',');
-- CALL export_table_to_csv('Friends','YOUR_ABSOLUTLY_PATH_TO_FILE/csv/', ',');
-- TRUNCATE TABLE  Friends;
-- CALL proc_import_from_csv('FrieNds','YOUR_ABSOLUTLY_PATH_TO_FILE/csv/Friends.csv', ',')

-- tests 2 export/import !!!!!!!

    --- Clean tables (!need add other tables!) ---
-- TRUNCATE TABLE Peers CASCADE; -- очистит все таблицы которые с ней связаны внешним ключом
-- TRUNCATE TABLE Friends;
-- TRUNCATE TABLE TransferredPoints;
-- TRUNCATE TABLE Recommendations;
-- TRUNCATE TABLE TimeTracking;
-- TRUNCATE TABLE Verter;
-- TRUNCATE TABLE p2p;
-- TRUNCATE TABLE XP;
-- TRUNCATE TABLE Checks CASCADE
-- TRUNCATE TABLE Tasks CASCADE

-- DROP SEQUENCE IF EXISTS id_for_checks CASCADE ;
-- DROP SEQUENCE IF EXISTS id_for_p2p CASCADE;
-- DROP SEQUENCE IF EXISTS id_for_verter CASCADE;
-- DROP SEQUENCE IF EXISTS id_for_xp CASCADE;

-- CREATE SEQUENCE id_for_p2p;
-- CREATE SEQUENCE id_for_checks;
-- CREATE SEQUENCE id_for_verter;
-- CREATE SEQUENCE id_for_xp;

-- CALL proc_import_from_csv('peers', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/peers.csv', ',');
-- CALL proc_import_from_csv('tasks', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/tasks.csv', ',');
-- CALL proc_import_from_csv('friends', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/friends.csv', ',');
-- CALL proc_import_from_csv('recommendations', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/recommendations.csv', ',');
-- CALL proc_import_from_csv('timetracking', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/timetracking.csv', ',');

-- CALL proc_import_from_csv('checks', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/checks.csv', ',');
-- CALL proc_import_from_csv('p2p', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/p2p.csv', ',');
-- CALL proc_import_from_csv('verter', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/verter.csv', ',');
-- CALL proc_import_from_csv('transferredpoints', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/transferredpoints.csv', ',');
-- CALL proc_import_from_csv('xp', 'YOUR_ABSOLUTLY_PATH_TO_FILE/csv/xp.csv', ',');

-- CALL proc_export_all_tables_to_csv('YOUR_ABSOLUTLY_PATH_TO_FILE/csv/', ',');
-- CALL proc_export_table_to_csv('Friends','YOUR_ABSOLUTLY_PATH_TO_FILE/csv/', ',');
-- TRUNCATE TABLE  Friends;
-- CALL proc_import_from_csv('FrieNds','YOUR_ABSOLUTLY_PATH_TO_FILE/csv/Friends.csv', ',')
