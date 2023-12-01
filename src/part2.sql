-- DROP ALL

DROP PROCEDURE IF EXISTS proc_dependency_lookup(peer_dep VARCHAR, task_dep VARCHAR, state_dep check_state);
DROP PROCEDURE IF EXISTS proc_must_have_project_done(peer_dep VARCHAR, task_dep VARCHAR);
DROP PROCEDURE IF EXISTS proc_must_have_project_done(peer_dep VARCHAR, task_dep VARCHAR);
DROP PROCEDURE IF EXISTS proc_dependency_lookup_for_verter(id_to_check BIGINT, state_dep check_state);
DROP PROCEDURE IF EXISTS proc_adding_verter(peer_from_checks VARCHAR, task_from_checks VARCHAR, state_to_verter check_state, time_to_verter time);
DROP PROCEDURE IF EXISTS proc_adding_p2p(checked_peer_checks VARCHAR, checking_peer_p2p VARCHAR, "title_tasks" VARCHAR, state_p2p check_state, time_to_checks time);

DROP TRIGGER IF EXISTS trg_person_points ON p2p CASCADE;
DROP FUNCTION IF EXISTS fnc_trg_TransferredPoints();

DROP TRIGGER IF EXISTS trg_pre_adding_xp ON p2p CASCADE;
DROP FUNCTION IF EXISTS fnc_trg_TransferredPoints();

--------------------------------------------------------
------------------------  ex01  ------------------------
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_must_have_project_done(peer_dep VARCHAR(255), task_dep VARCHAR(255))
LANGUAGE plpgsql AS
$MUST_HAVE_PROJECT_DONE$
DECLARE state_of_must_have_project check_state;
BEGIN WITH needed_task AS (
    SELECT parent_task accept_task
    FROM tasks
    WHERE title = task_dep
),
needed_id_of_checks AS (
    SELECT c.id
    FROM checks c
        JOIN needed_task nt ON nt.accept_task = c.task
    WHERE c.peer = peer_dep
    ORDER BY date DESC
    LIMIT 1
)
SELECT * INTO state_of_must_have_project
FROM xp
    JOIN needed_id_of_checks nc ON nc.id = xp."check";
IF state_of_must_have_project IS NULL THEN RAISE EXCEPTION 'Must have project not done';
END IF;
END;
$MUST_HAVE_PROJECT_DONE$;

----------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_dependency_lookup(
        peer_dep VARCHAR(255),
        task_dep VARCHAR(255),
        state_dep check_state
    ) LANGUAGE plpgsql AS
$DEPENDENCY_LOOKUP$
DECLARE id_for_lookup BIGINT := (
        SELECT max(c.id)
        FROM checks c
        WHERE c.peer = peer_dep
            AND c.task = task_dep
    );
state_for_lookup check_state := (
    SELECT p.state
    FROM p2p p
    WHERE id_for_lookup = p."check"
    ORDER BY p.time DESC
    LIMIT 1
);
BEGIN CALL proc_must_have_project_done(peer_dep, task_dep);
IF (
    (
        state_dep != 'start'
        AND state_for_lookup != 'start'
    )
    OR (
        state_dep = 'start'
        AND state_for_lookup = 'start'
    )
) THEN RAISE EXCEPTION 'The inserted data is not consistent with the data posted earlier in the p2p verification table ';
END IF;
DROP TABLE IF EXISTS state_for_lookup;
END;
$DEPENDENCY_LOOKUP$;

-------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_adding_p2p(
        checked_peer_checks VARCHAR(255),
        checking_peer_p2p VARCHAR(255),
        "title_tasks" VARCHAR(255),
        state_p2p check_state,
        time_to_checks time
    ) LANGUAGE plpgsql AS
$ADDING_P2P$
DECLARE needed_id BIGINT DEFAULT (
        SELECT max(c2.id)
        FROM p2p p
            JOIN checks c2 ON p."check" = c2.id
            AND c2.peer = checked_peer_checks
            AND c2.task = "title_tasks"
            AND p.checkingpeer = checking_peer_p2p
    );
BEGIN CALL proc_dependency_lookup(checked_peer_checks, "title_tasks", state_p2p);
IF state_p2p = 'start' THEN
INSERT INTO checks (peer, task, date)
VALUES (
        checked_peer_checks,
        "title_tasks",
        current_date::date
    );
INSERT INTO p2p ("check", checkingpeer, state, time)
VALUES (
        (
            SELECT max(id)
            FROM checks c
        ),
        checking_peer_p2p,
        state_p2p,
        time_to_checks
    );
ELSE
INSERT INTO p2p ("check", checkingpeer, state, time)
VALUES (
        needed_id,
        checking_peer_p2p,
        state_p2p,
        time_to_checks
    );
END IF;
END;
$ADDING_P2P$;

-- ************************************************ --
------------------- tests ex01 -----------------------
------------------------------------------------------

SELECT *
FROM checks c
         JOIN p2p p on c.id = p."check"
WHERE c.peer = 'troybrown'
  AND c.task = 'CPP2_s21_containers';

CALL proc_adding_p2p('troybrown', 'laurenwood', 'CPP2_s21_containers', 'start', current_time::time);

SELECT *
FROM checks c
         JOIN p2p p on c.id = p."check"
WHERE c.peer = 'troybrown'
  AND c.task = 'CPP2_s21_containers';

-- проверка на то, что запись не добавится, т.к. родительский проект DO1_Linux невыполнен
CALL proc_adding_p2p('troybrown', 'laurenwood', 'DO2_Linux_Network', 'start', current_time::time);

SELECT *
FROM checks c
         JOIN p2p p on c.id = p."check"
WHERE c.peer = 'troybrown'
  AND c.task = 'DO1_Linux';

-- проверка на то, что что записи не добавятся, в случае если в таблице ХР не начислен опыт за родитлеьский проект

CALL proc_adding_p2p('laurenwood', 'troybrown', 'CPP2_s21_containers', 'start', current_time::time);

--------------------------------------------------------
------------------------  ex02  ------------------------
--------------------------------------------------------

WITH needed_task AS (
    SELECT parent_task accept_task
    FROM tasks
    WHERE title = 'CPP2_s21_containers'
),
needed_id_of_checks AS (
    SELECT c.id
    FROM checks c
        JOIN needed_task nt ON nt.accept_task = c.task
    WHERE c.peer = 'laurenwood'
    ORDER BY date DESC
    LIMIT 1
)
SELECT *
FROM xp
    JOIN needed_id_of_checks nc ON nc.id = xp."check";

CREATE OR REPLACE PROCEDURE proc_dependency_lookup_for_verter(id_to_check BIGINT, state_dep check_state)
LANGUAGE plpgsql AS
$DEPENDENCY_LOOKUP_VERTER$
DECLARE state_for_lookup check_state;
    start_eq check_state := 'start';
BEGIN
    SELECT "state" INTO state_for_lookup
    FROM verter v
    WHERE v."check" = id_to_check
    ORDER BY "time" DESC
    LIMIT 1;
    IF id_to_check IS NULL THEN RAISE EXCEPTION '1 - There is no check in CHECKS and P2P TABLES with this parametrs';
    ELSIF (
        (
            state_dep = start_eq
            AND state_for_lookup = start_eq
        )
        OR (
            state_dep != start_eq
            AND state_for_lookup != start_eq
        )
    ) THEN RAISE EXCEPTION '2 - The inserting data is already inserted in VERTER TABLE';
    ELSIF (
        state_dep = start_eq
        AND id_to_check IN (
            SELECT v."check"
            FROM verter v
            WHERE v.state = state_dep
        )
    ) THEN RAISE EXCEPTION '3 - This verter check was already started in VERTER TABLE';
    ELSIF (
        state_dep != start_eq
        AND (
            SELECT v.state
            FROM verter v
            WHERE v."check" = id_to_check
            ORDER BY 1
            LIMIT 1
        ) != start_eq
    ) THEN RAISE EXCEPTION '4 - This verter check was already ended in VERTER TABLE';
    ELSIF (
        SELECT p.state
        FROM p2p p
        WHERE p."check" = id_to_check
        ORDER BY p.time DESC
        LIMIT 1
    ) != 'success'::check_state THEN RAISE EXCEPTION '5 - For this id from CHECKS TABLE there is no success passed p2p check in P2P TABLE';
    END IF;
END;
$DEPENDENCY_LOOKUP_VERTER$;

CREATE OR REPLACE PROCEDURE proc_adding_verter(
        peer_from_checks VARCHAR(255),
        task_from_checks VARCHAR(255),
        "state_to_verter" check_state,
        "time_to_verter" time
    ) LANGUAGE plpgsql AS
$ADDING_VERTER$
DECLARE id_from_checks BIGINT;
BEGIN
SELECT c.id foreign_key_for_verter INTO id_from_checks
FROM checks c
    JOIN p2p p on c.id = p."check"
    AND c.peer = peer_from_checks
    AND c.task = task_from_checks
ORDER BY c.date DESC,
    p.time DESC
LIMIT 1;
CALL proc_dependency_lookup_for_verter(id_from_checks, "state_to_verter");
INSERT INTO verter ("check", "state", "time")
VALUES (
        id_from_checks,
        state_to_verter,
        "time_to_verter"
    );
END;
$ADDING_VERTER$;

-- ************************************************ --
------------------- tests ex02 -----------------------
------------------------------------------------------
-- проверка на то, что не добавляется запись, в случае, если нет успешной по чек проверки в таблице п2п

CALL proc_adding_verter('troybrown', 'CPP2_s21_containers', 'success' , current_time::time);

-- проверка на то, что данные повторно не добавятся в таблицу вертер

SELECT *
FROM checks c
         JOIN verter v on c.id = v."check"
WHERE peer = 'kennethgraham'
  AND task = 'CPP2_s21_containers';

CALL proc_adding_verter('kennethgraham', 'CPP2_s21_containers', 'start' , current_time::time);

-- проверка на то, что данные, которых нет в таблицу п2п не добавятся
CALL proc_adding_verter('josepayne', 'CPP2_s21_containers', 'start' , current_time::time);


--------------------------------------------------------
------------------------  ex03  ------------------------
--------------------------------------------------------
CREATE OR REPLACE FUNCTION  fnc_trg_TransferredPoints()
RETURNS TRIGGER AS
$TransferredPoints$
DECLARE
TO_BE BIGINT := (select t.id::BIGINT
            From p2p As p
            JOIN checks AS c ON c.id = p.check
            join transferredpoints AS t ON c.peer = t.checkedpeer AND p.checkingpeer = t.checkingpeer
            WHERE p.state = 'start' AND p.check = NEW."check");
    BEGIN
        IF (TG_OP = 'INSERT' AND NEW."state" = 'start') THEN
            IF ( TO_BE != 0 ) THEN
                UPDATE TransferredPoints SET pointsamount = pointsamount + 1 WHERE id =  TO_BE;
            ELSE
                INSERT INTO TransferredPoints (CheckingPeer,CheckedPeer, PointsAmount)
                SELECT p2p.CheckingPeer, Checks.Peer, 1
                FROM p2p JOIN checks ON p2p.Check = Checks.Id
                WHERE p2p."state" = 'start' AND p2p.check = NEW."check";
            END IF;
        END IF;
        RETURN NULL;
    END;
$TransferredPoints$
LANGUAGE plpgsql;

CREATE TRIGGER trg_person_points
AFTER INSERT ON p2p
    FOR EACH ROW EXECUTE FUNCTION fnc_trg_TransferredPoints();



-- ************************************************ --
------------------- tests ex03 -----------------------
------------------------------------------------------

-----
----- test 01
-----
SELECT * FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray';

INSERT INTO checks (id,peer, task, date)
VALUES ( 100,'frankray','A1_MAZE', CURRENT_DATE);
INSERT INTO p2p ("check", checkingpeer, "state", "time")
VALUES (100,'lorigarrett','start',current_time),
(100,'lorigarrett','success',current_time + '00:25:30'::TIME);

SELECT * FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray';
DELETE FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray'; 
DELETE FROM p2p WHERE "check" = 100;
DELETE FROM checks WHERE id = 100;
-----
----- test 02
-----
INSERT INTO checks (id,peer, task, date)
VALUES ( 101,'frankray','A2_SimpleNavigator v1.0', CURRENT_DATE);
INSERT INTO p2p ("check", checkingpeer, "state", "time")
VALUES (101,'lorigarrett','start',current_time),
(101,'lorigarrett','success',current_time + '00:25:30'::TIME);

SELECT * FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray';

DELETE FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray';
DELETE FROM p2p WHERE "check" = 101;
DELETE FROM checks WHERE id = 101;
-----
----- test 03
-----
INSERT INTO checks (id,peer, task, date)
VALUES ( 102,'lorigarrett','C6_s21_matrix', CURRENT_DATE);
INSERT INTO p2p ("check", checkingpeer, "state", "time")
VALUES (102,'frankray','start',current_time),
(102,'frankray','fail',current_time + '00:25:30'::TIME);

SELECT * FROM transferredpoints WHERE( checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray') OR  (checkedpeer  = 'lorigarrett' AND checkingpeer = 'frankray');
DELETE FROM transferredpoints WHERE checkedpeer = 'lorigarrett' AND checkingpeer = 'frankray';
DELETE FROM p2p WHERE "check" = 102;
DELETE FROM checks WHERE id = 102;



--------------------------------------------------------
------------------------  ex04  ------------------------
--------------------------------------------------------


CREATE OR REPLACE FUNCTION fnc_trg_insert_xp() RETURNS TRIGGER
    LANGUAGE plpgsql AS
$INSERT_XP$
         BEGIN
    IF NOT (fnc_p2p_or_verter_success(NEW."check")) THEN
        RAISE EXCEPTION 'Autotest VERTER = ''fail'' for verification %', NEW."check";
        RETURN NULL;
        ELSE
            IF NOT (fnc_xp_lq_max(NEW."check", NEW.xp_amount)) THEN
                RAISE EXCEPTION 'More than max_xp in check %', NEW."check";
                ELSE
                RETURN NEW;
            END IF;
            END IF;
            END;
$INSERT_XP$;

CREATE TRIGGER trg_pre_adding_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_insert_xp();


-- ************************************************ --
------------------- tests ex04 -----------------------
------------------------------------------------------

-----
----- test 01
-----
INSERT INTO xp ("check", XP_amount)
VALUES (100,301);
SELECT * FROM xp WHERE "check" = 100;
DELETE FROM xp WHERE "check" = 100;
-----
----- test 02
-----
DELETE FROM xp WHERE "check" = 101;
SELECT * FROM xp WHERE "check" = 101;
INSERT INTO xp ("check", XP_amount)
VALUES (101,400);
-----
----- test 03
-----
DELETE FROM xp WHERE "check" = 100;
DELETE FROM verter WHERE "check" = 100;
INSERT INTO verter (id, "check", "state", "time")
VALUES (1000,100,'start','10:45:03'),
        (1001,100,'fail','10:45:45');
SELECT * FROM verter WHERE "check" = 100;
INSERT INTO xp ("check", XP_amount)
VALUES (100,300);
SELECT * FROM xp WHERE "check" = 100;


