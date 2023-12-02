--------------------------------------------------------
---------------------- DROP ALL ------------------------
--------------------------------------------------------

DROP PROCEDURE IF EXISTS proc_dependency_lookup(peer_dep VARCHAR, task_dep VARCHAR, state_dep check_state);
DROP PROCEDURE IF EXISTS proc_must_have_project_done(peer_dep VARCHAR, task_dep VARCHAR);
DROP PROCEDURE IF EXISTS proc_must_have_project_done(peer_dep VARCHAR, task_dep VARCHAR);
DROP PROCEDURE IF EXISTS proc_dependency_lookup_for_verter(id_to_check BIGINT, state_dep check_state);
DROP TRIGGER IF EXISTS trg_person_points ON p2p CASCADE;
DROP TRIGGER IF EXISTS trg_pre_adding_xp ON p2p CASCADE;
DROP PROCEDURE IF EXISTS proc_adding_p2p(checked_peer_checks VARCHAR, checking_peer_p2p VARCHAR, "title_tasks" VARCHAR, state_p2p check_state, time_to_checks time);
DROP FUNCTION IF EXISTS fnc_trg_TransferredPoints();

DROP PROCEDURE IF EXISTS proc_parrent_task_done(title_for_checks varchar, peer_for_checks varchar);
DROP PROCEDURE IF EXISTS proc_is_it_new_check(title_for_checks varchar, peer_for_checks varchar, state_for_p2p check_state);
DROP PROCEDURE IF EXISTS proc_peers_not_eq(peer_for_checks VARCHAR(255), peer_for_p2p VARCHAR(255));
DROP PROCEDURE IF EXISTS proc_insert_date_into_p2p(peer_for_p2p VARCHAR(255), state_for_p2p check_state,
                                                      time_for_p2p time, checks_id BIGINT);
DROP PROCEDURE IF EXISTS proc_adding_verter(peer_from_checks VARCHAR, task_from_checks VARCHAR, state_to_verter check_state, time_to_verter time);
DROP PROCEDURE IF EXISTS proc_check_in_checks_and_success_in_p2p(peer_from_checks VARCHAR(255), task_from_checks VARCHAR(255), checks_id BIGINT);
DROP PROCEDURE IF EXISTS proc_is_it_new_check_verter("state_to_verter" check_state, checks_id BIGINT);

--------------------------------------------------------
------------------------  ex01  ------------------------
--------------------------------------------------------


CREATE OR REPLACE PROCEDURE proc_parrent_task_done(
        title_for_checks VARCHAR(255),
        peer_for_checks VARCHAR(255)
    ) LANGUAGE plpgsql AS 
$$
DECLARE 
    parrent_task VARCHAR(255);
    parrent_task_checks_id BIGINT;
    xp_for_parrent_task SMALLINT;
BEGIN 
    RAISE INFO 'Start procedure to check parrent task dor % is done', title_for_checks;
    parrent_task := (
        SELECT t.parent_task pt
        FROM tasks t
        WHERE t.title = title_for_checks
    );
    parrent_task_checks_id := (
        SELECT c.id
        FROM checks c
        WHERE c.peer = peer_for_checks
            AND c.task = parrent_task
        ORDER BY c.date DESC
        LIMIT 1
    );
    xp_for_parrent_task := (
        SELECT xp_amount
        FROM xp
        WHERE xp."check" = parrent_task_checks_id
    );
    IF xp_for_parrent_task IS NULL THEN 
        RAISE 'Parrent task for % is not done', title_for_checks;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE proc_is_it_new_check(
        title_for_checks VARCHAR(255),
        peer_for_checks VARCHAR(255),
        state_for_p2p check_state
    ) LANGUAGE plpgsql AS
$$
DECLARE
    start_state check_state DEFAULT 'start';
    last_checks_id_with_this_params BIGINT;
    p2p_state_for_checks_id check_state;
BEGIN 
    RAISE INFO 'Start procedure to check p2p with parametrs % and % and NOT is in state = start', peer_for_checks, title_for_checks;
    last_checks_id_with_this_params := (
        SELECT c.id
        FROM checks c
        WHERE c.peer = peer_for_checks
            AND c.task = title_for_checks
        ORDER BY c.date DESC
        LIMIT 1
    );
    p2p_state_for_checks_id := (
        SELECT p.state
        FROM p2p p
        WHERE p."check" = last_checks_id_with_this_params
        ORDER BY p.time DESC
        LIMIT 1
    );
    IF (
        p2p_state_for_checks_id != start_state
        AND state_for_p2p != start_state
    )
    OR (
        p2p_state_for_checks_id = start_state
        AND state_for_p2p = start_state
    )
    OR (
        last_checks_id_with_this_params IS NULL
        AND state_for_p2p != start_state
    ) THEN 
        RAISE 'The check with this parametrs: % and % already been started or ended',
        peer_for_checks,
        title_for_checks USING HINT = 'You need to end opened check with this checks_id in table p2p or start new check. Your variant depending on parametr of state in new row of check';
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE proc_peers_not_eq(
        peer_for_checks VARCHAR(255),
        peer_for_p2p VARCHAR(255)
    ) LANGUAGE plpgsql AS
$$
BEGIN 
    RAISE INFO 'Start procedure thats compares peers nicknames';
    IF peer_for_checks = peer_for_p2p THEN 
        RAISE EXCEPTION 'The peer can''t check itself';
    END IF;
END;
$$;
CREATE OR REPLACE PROCEDURE proc_insert_date_into_checks(
        peer_for_checks VARCHAR(255),
        title_for_checks VARCHAR(255),
        state_for_p2p check_state
    ) LANGUAGE plpgsql AS
$$
BEGIN 
    IF state_for_p2p = 'start'::check_state THEN
        INSERT INTO checks(peer, task, date)
        VALUES (
                peer_for_checks,
                title_for_checks,
                current_date::date
            );
        RAISE INFO 'Adding date in checks is done';
    END IF;
END;
$$;
CREATE OR REPLACE PROCEDURE proc_insert_date_into_p2p(
        peer_for_p2p VARCHAR(255),
        state_for_p2p check_state,
        time_for_p2p time,
        checks_id BIGINT
    ) LANGUAGE plpgsql AS 
$$
BEGIN
    INSERT INTO p2p("check", checkingpeer, "state", "time")
    SELECT checks_id,
        peer_for_p2p,
        state_for_p2p,
        time_for_p2p;
    RAISE INFO 'Adding date in p2p is done';
END;
$$;
CREATE OR REPLACE PROCEDURE prc_adding_p2p(
        peer_for_checks VARCHAR(255),
        peer_for_p2p VARCHAR(255),
        title_for_checks VARCHAR(255),
        state_for_p2p check_state,
        time_for_p2p time
    ) LANGUAGE plpgsql AS
$ADDING_P2P$
DECLARE
    checks_id BIGINT;
DECLARE BEGIN CALL proc_parrent_task_done(title_for_checks, peer_for_checks);
    CALL proc_is_it_new_check(title_for_checks, peer_for_checks, state_for_p2p);
    CALL proc_peers_not_eq(peer_for_checks, peer_for_p2p);
    CALL proc_insert_date_into_checks(peer_for_checks, title_for_checks, state_for_p2p);
    checks_id := (
        SELECT c.id
        FROM checks c
        WHERE c.peer = peer_for_checks
            AND c.task = title_for_checks
    );
    CALL proc_insert_date_into_p2p(
        peer_for_p2p,
        state_for_p2p,
        time_for_p2p,
        checks_id
    );
END;
$ADDING_P2P$;

-- ************************************************ --
------------------- tests ex01 -----------------------
------------------------------------------------------

--
-- Test that the date was insert
--
SELECT *
FROM checks c,
     p2p p
WHERE c.id = p."check"
  AND c.peer = 'pamelawalker'
  AND p.checkingpeer = 'troybrown'
  AND c.task = 'A1_MAZE';
--
-- adding checks && p2p
--
CALL prc_adding_p2p('pamelawalker', 'troybrown','A1_MAZE', 'start', '11:11:11');

SELECT *
FROM checks c,
     p2p p
WHERE c.id = p."check"
  AND c.peer = 'pamelawalker'
  AND p.checkingpeer = 'troybrown'
  AND c.task = 'A1_MAZE';
--
-- adding just p2p
--
CALL prc_adding_p2p('pamelawalker', 'troybrown','A1_MAZE', 'success', '12:11:11');

SELECT *
FROM checks c,
     p2p p
WHERE c.id = p."check"
  AND c.peer = 'pamelawalker'
  AND p.checkingpeer = 'troybrown'
  AND c.task = 'A1_MAZE';
--
-- Test adding project that hoven-t got parrent task is done.
--
CALL prc_adding_p2p('pamelawalker', 'troybrown','DO3_LinuxMonitoring_v1.0', 'start', '11:11:11');
--
-- Test thats demonstrate when date with same parametrs does not insert
--
CALL prc_adding_p2p('pamelawalker', 'troybrown','A1_MAZE', 'start', '11:11:11');
--
-- Test thats demonstrate when peers with same names is blocked
--
CALL prc_adding_p2p('frankray', 'frankray','A1_MAZE', 'start', '11:11:11');

--------------------------------------------------------
------------------------  ex02  ------------------------
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_check_in_checks_and_success_in_p2p(peer_from_checks VARCHAR(255), task_from_checks VARCHAR(255), checks_id BIGINT)
    LANGUAGE plpgsql AS
$$
    DECLARE
        p2p_state check_state;
        success_state check_state DEFAULT 'success';
BEGIN
        p2p_state := (SELECT p.state
                      FROM p2p p
                      WHERE p."check" = checks_id
                      ORDER BY p.time DESC
                      LIMIT 1);
        IF p2p_state != success_state THEN
            RAISE 'Check p2p with the peremetrs % and % is not success. Accrual of experience blocked', peer_from_checks, task_from_checks;
        END IF;
END ;
    $$;

CREATE OR REPLACE PROCEDURE proc_is_it_new_check_verter("state_to_verter" check_state, checks_id BIGINT)
    LANGUAGE plpgsql AS
$$
DECLARE
    verter_state check_state := (SELECT v.state
                                 FROM verter v
                                 WHERE v."check" = checks_id);
    start_state check_state DEFAULT 'start';
BEGIN
    IF (verter_state = start_state AND state_to_verter = start_state) OR
       (verter_state != start_state AND state_to_verter != start_state) OR
       (verter_state IS NULL AND state_to_verter != start_state)THEN
    RAISE 'The parametrs for verter check is bad, the check was started or ended, please pay attention on parametr ''%''', state_to_verter;
        END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE proc_insert_date_verter(state_to_verter check_state, time_to_verter time,
                                                    checks_id BIGINT)
    LANGUAGE plpgsql AS
$$
BEGIN
    INSERT INTO verter("check", state, time)
    VALUES (checks_id, state_to_verter, time_to_verter);
END;
$$;


CREATE OR REPLACE PROCEDURE proc_adding_verter(peer_from_checks VARCHAR(255), task_from_checks VARCHAR(255),
                                               state_to_verter check_state, time_to_verter time)
    LANGUAGE plpgsql AS
$$
DECLARE
    checks_id BIGINT := (SELECT c.id
                         FROM checks c
                         WHERE c.peer = peer_from_checks
                           AND c.task = task_from_checks);
BEGIN
    CALL proc_check_in_checks_and_success_in_p2p(peer_from_checks, task_from_checks, checks_id);
    CALL proc_is_it_new_check_verter(state_to_verter, checks_id);
    CALL proc_insert_date_verter(state_to_verter, time_to_verter, checks_id);
END;
$$;

-- ************************************************ --
------------------- tests ex02 -----------------------
------------------------------------------------------

--
-- Test demonstrate that date will be insert 
--

SELECT *
FROM checks c,
     verter v
WHERE c.peer = 'nancywilson'
  AND c.task = 'CPP2_s21_containers'
  AND v."check" = c.id;

CALL proc_adding_verter('nancywilson', 'CPP2_s21_containers', 'start', '22:22:22');

SELECT *
FROM checks c,
     verter v
WHERE c.peer = 'nancywilson'
  AND c.task = 'CPP2_s21_containers'
  AND v."check" = c.id;

--
-- Test demonstrate that date will not insert because of conflict state.
--

CALL proc_adding_verter('nancywilson', 'CPP2_s21_containers', 'start', '22:22:22');

--
-- Test demostrates that date will not be insert if p2p is fail
--

CALL prc_adding_p2p('frankray', 'nancywilson', 'A1_MAZE', 'start', '10:10:10');
CALL prc_adding_p2p('frankray', 'nancywilson', 'A1_MAZE', 'fail', '11:10:10');
CALL proc_adding_verter('frankray', 'A1_MAZE', 'start', '12:12:12');


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
-- DELETE FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray'; 
-- DELETE FROM p2p WHERE "check" = 100;
-- DELETE FROM checks WHERE id = 100;

-----
----- test 02
-----
INSERT INTO checks (id,peer, task, date)
VALUES ( 101,'frankray','A2_SimpleNavigator v1.0', CURRENT_DATE);
INSERT INTO p2p ("check", checkingpeer, "state", "time")
VALUES (101,'lorigarrett','start',current_time),
(101,'lorigarrett','success',current_time + '00:25:30'::TIME);

SELECT * FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray';

-- DELETE FROM transferredpoints WHERE checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray';
-- DELETE FROM p2p WHERE "check" = 101;
-- DELETE FROM checks WHERE id = 101;

-----
----- test 03
-----
INSERT INTO checks (id,peer, task, date)
VALUES ( 102,'lorigarrett','C6_s21_matrix', CURRENT_DATE);
INSERT INTO p2p ("check", checkingpeer, "state", "time")
VALUES (102,'frankray','start',current_time),
(102,'frankray','fail',current_time + '00:25:30'::TIME);

SELECT * FROM transferredpoints WHERE( checkingpeer = 'lorigarrett' AND checkedpeer = 'frankray') OR  (checkedpeer  = 'lorigarrett' AND checkingpeer = 'frankray');
-- DELETE FROM transferredpoints WHERE checkedpeer = 'lorigarrett' AND checkingpeer = 'frankray';
-- DELETE FROM p2p WHERE "check" = 102;
-- DELETE FROM checks WHERE id = 102;



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






--- proba

-- CREATE OR REPLACE PROCEDURE proc_adding_p2p_check(
--     _checkedpeer VARCHAR,
--     _checkingpeer VARCHAR,
--     _task VARCHAR,
--     _state check_state DEFAULT 'start',
--     _time TIME DEFAULT current_time
-- ) LANGUAGE plpgsql AS
-- $ADDING_P2P_CHECK$
-- DECLARE
--     id_for_table_checks BIGINT DEFAULT (SELECT MAX(id)+1 FROM checks)::BIGINT;
--     id_for_table_p2p BIGINT DEFAULT (SELECT MAX(id)+1 FROM p2p)::BIGINT;
--     parent_t VARCHAR DEFAULT  (SELECT parent_task FROM tasks WHERE title =  _task);
--     status_parent_task BIGINT DEFAULT 0;

-- BEGIN

--     IF  parent_t = NULL THEN
--         status_parent_task := 100;
--     ELSE
--         status_parent_task := (SELECT x.xp_amount::NUMERIC / t.max_xp * 100.0
--         FROM checks AS ch JOIN tasks AS t ON t.title = ch.task JOIN p2p AS p ON ch.id = p."check" AND p."state" = 'success' JOIN xp AS x ON x.check = ch.id
--         WHERE ch.peer = _checkedpeer AND ch.task =  parent_t  
--         AND ch.id = (select xp.check FROM xp WHERE ch.id = xp.check) ORDER BY ch."date" DESC, p."time" DESC LIMIT 1)::BIGINT;
--     END IF;
-- IF _state = 'start' AND status_parent_task > 80 AND (SELECT count(*) %2 FROM p2p WHERE "check" = (SELECT ch.id FROM p2p AS p 
--                                         INNER JOIN checks AS ch ON ch.id = p.check
--                                         WHERE ch.peer = _checkedpeer 
--                                             AND ch.task = _task
--                                             AND p.checkingpeer = _checkingpeer
--                                         ORDER BY ch."date" DESC,
--                                             p.time DESC
--                                         LIMIT 1)::BIGINT)::BIGINT = 0  THEN
--     INSERT INTO checks (id, peer, task, "date") 
--     VALUES (id_for_table_checks, _checkedpeer, _task, CURRENT_DATE);
--     INSERT INTO p2p (id, "check", checkingpeer, "state", "time")
--     VALUES (id_for_table_p2p, id_for_table_checks, _checkingpeer, _state, _time );
-- ELSIF _state != 'start' THEN 
--     id_for_table_checks := (SELECT ch.id FROM p2p AS p 
--                                         INNER JOIN checks AS ch ON ch.id = p.check
--                                         WHERE ch.peer = _checkedpeer 
--                                             AND ch.task = _task
--                                             AND p.checkingpeer = _checkingpeer
--                                         ORDER BY ch."date" DESC,
--                                             p.time DESC
--                                         LIMIT 1)::BIGINT;
--     IF (SELECT count(*) FROM p2p WHERE "check" = id_for_table_checks )::BIGINT = 1 THEN
--         INSERT INTO p2p (id, "check", checkingpeer, "state", "time")
--         VALUES (id_for_table_p2p, id_for_table_checks, _checkingpeer, _state, _time );
--     ELSIF (SELECT count(*) FROM p2p WHERE "check" = id_for_table_checks ) > 1 THEN 
--         RAISE EXCEPTION 'The VERIFICATION of the % peer with the % project has already been recorded.', _checkedpeer,_task;
--     ELSE 
--         RAISE INFO 'ERROR ERROR ERROR';
--     END IF;
-- END IF;
-- END;
-- $ADDING_P2P_CHECK$;


--     SELECT *
--     FROM checks c
--          JOIN p2p p on c.id = p."check"
-- WHERE c.peer = 'troybrown'
--   AND c.task = 'CPP2_s21_containers';

-- CALL proc_adding_p2p_check('troybrown', 'laurenwood', 'CPP2_s21_containers', 'fail', current_time::time);