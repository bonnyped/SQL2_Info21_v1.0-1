CREATE OR REPLACE PROCEDURE dependency_lookup(peer_dep VARCHAR(255), task_dep VARCHAR(255), state_dep check_state)
LANGUAGE plpgsql AS $DEPENDENCY_LOOKUP$
DECLARE
    id_for_lookup    BIGINT;
    state_for_lookup check_state;
BEGIN
    SELECT max(c.id)
    INTO id_for_lookup
    FROM checks c
    WHERE c.peer = peer_dep AND c.task = task_dep;
    SELECT p.state
    INTO state_for_lookup
    FROM p2p p
    WHERE id_for_lookup = p."check"
    ORDER BY p.time DESC
    LIMIT 1;
    IF ((state_dep != 'start' AND state_for_lookup != 'start') OR (state_dep = 'start' AND state_for_lookup = 'start'))
        THEN
        RAISE EXCEPTION 'The inserted data is not consistent with the data posted earlier in the p2p verification table ';
    END IF;
END;
    $DEPENDENCY_LOOKUP$;

CREATE OR REPLACE PROCEDURE adding_p2p(checked_peer_checks  VARCHAR(255), checking_peer_p2p  VARCHAR(255),
                                       task_checks  VARCHAR(255), state_p2p check_state, time_to_checks time)
    LANGUAGE plpgsql AS $ADDING_P2P$
    DECLARE
        needed_id BIGINT DEFAULT (SELECT max(c2.id)
                       FROM p2p p
                                JOIN checks c2 ON p."check" = c2.id AND c2.peer = checked_peer_checks AND
                                                  c2.task = task_checks AND p.checking_peer = checking_peer_p2p);
    BEGIN
        CALL dependency_lookup(checked_peer_checks, task_checks, state_p2p);
        IF state_p2p = 'start' THEN
           INSERT INTO checks (peer, task, date)
           VALUES (checked_peer_checks, task_checks, current_date::date);
           INSERT INTO p2p ("check", checking_peer, state, time)
           VALUES ((SELECT max(id) FROM checks c), checking_peer_p2p, state_p2p, time_to_checks);
        ELSE INSERT INTO p2p ("check", checking_peer, state, time)
             VALUES (needed_id, checking_peer_p2p, state_p2p, time_to_checks);
        END IF;
    END;
    $ADDING_P2P$;

-- CALL adding_p2p('troybrown', 'laurenwood', 'CPP2_s21_containers', 'start', current_time::time);

CREATE OR REPLACE PROCEDURE dependency_lookup_for_verter(id_to_check BIGINT, state_dep check_state)
LANGUAGE plpgsql AS $DEPENDENCY_LOOKUP_VERTER$
    DECLARE
        state_for_lookup check_state;
        start_eq check_state := 'start';
    BEGIN
        SELECT "state"
        INTO state_for_lookup
            FROM verter v
        WHERE v."check" = id_to_check
        ORDER BY "time" DESC
        LIMIT 1;
        IF id_to_check IS NULL THEN
            RAISE EXCEPTION '1 - There is no check in CHECKS and P2P TABLES with this parametrs'; 
        ELSIF ((state_dep = start_eq AND state_for_lookup = start_eq) OR (state_dep != start_eq
            AND state_for_lookup != start_eq)) THEN
            RAISE EXCEPTION '2 - The inserting data is already inserted in VERTER TABLE';
        ELSIF (state_dep = start_eq AND
               id_to_check IN (SELECT v."check" FROM verter v WHERE v.state = state_dep)) THEN
            RAISE EXCEPTION '3 - This verter check was already started in VERTER TABLE';
        ELSIF (state_dep != start_eq AND
               (SELECT v.state FROM verter v WHERE v."check" = id_to_check ORDER BY 1 LIMIT 1) !=
               start_eq) THEN
            RAISE EXCEPTION '4 - This verter check was already ended in VERTER TABLE';
        ELSIF (SELECT p.state FROM p2p p WHERE p."check" = id_to_check ORDER BY p.time DESC LIMIT 1) !=
               'success'::check_state THEN
            RAISE EXCEPTION '5 - For this id from CHECKS TABLE there is no success passed p2p check in P2P TABLE';
        END IF;
        END;
    $DEPENDENCY_LOOKUP_VERTER$;

CREATE OR REPLACE PROCEDURE adding_verter(peer_from_checks VARCHAR(255), task_from_checks VARCHAR(255), "state_to_verter" check_state, "time_to_verter" time)
LANGUAGE plpgsql AS $ADDING_VERTER$
    DECLARE
        id_from_checks BIGINT;
    BEGIN
        SELECT c.id foreign_key_for_verter
        INTO id_from_checks
        FROM checks c
        JOIN p2p p on c.id = p."check" AND c.peer = peer_from_checks AND c.task = task_from_checks
        ORDER BY c.date DESC, p.time DESC
        LIMIT 1;
        CALL dependency_lookup_for_verter(id_from_checks, "state_to_verter");
        INSERT INTO verter ("check", "state", "time")
        VALUES (id_from_checks, state_to_verter, "time_to_verter");
    END;
    $ADDING_VERTER$;

-- CALL adding_verter('troybrown', 'CPP2_s21_containers', 'success' , current_time::time);
-- 
-- DELETE FROM verter where id = 28;
-- 
-- DROP PROCEDURE adding_verter(peer_from_checks VARCHAR, task_from_checks VARCHAR, state_to_verter check_state, time_to_verter time);

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

CREATE TRIGGER trg_person_audit
AFTER INSERT ON p2p
    FOR EACH ROW EXECUTE FUNCTION fnc_trg_TransferredPoints();
	

