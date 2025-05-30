
DROP FUNCTION IF EXISTS fnc_TransferredPointsStatistic;
DROP FUNCTION IF EXISTS fnc_checks_task_xp;
DROP FUNCTION IF EXISTS fnc_hardworking_peers;
DROP FUNCTION IF EXISTS  fnc_points_traffic_all;
DROP FUNCTION IF EXISTS  fnc_points_traffic;
DROP FUNCTION IF EXISTS fnc_point_changes;
DROP FUNCTION IF EXISTS fnc_recommendation_peer;
DROP FUNCTION IF EXISTS fnc_status_checks_procent;
DROP PROCEDURE IF EXISTS prc_third_task_not_completed(
   firsttask VARCHAR,
   secondtask VARCHAR,
   thirdtask VARCHAR,
   IN _result_one refcursor
);
DROP PROCEDURE IF EXISTS prc_find_good_days_for_checks;


--------------------------------------------
-------------------- 01 --------------------
--------------------------------------------

CREATE OR REPLACE FUNCTION fnc_TransferredPointsStatistic() RETURNS TABLE(
        "Peer1" VARCHAR,
        "Peer2" VARCHAR,
        "PointsAmount" BIGINT
    ) LANGUAGE plpgsql AS
$STATISTIC_POINTS$
BEGIN
RETURN QUERY
WITH mutual_checks AS (
    SELECT tp.id,
        tp.checkingpeer AS Peer1,
        tp.checkedpeer AS Peer2,
        (tp.Pointsamount - tp2.Pointsamount) AS Pointsamount
    FROM TransferredPoints AS tp
        JOIN transferredpoints AS tp2 ON tp.checkingpeer = tp2.checkedpeer
        AND tp.checkedpeer = tp2.checkingpeer
        AND tp.id != tp2.id
),
non_reciprocal_checks AS (
    SELECT DISTINCT tp3.id,
        tp3.checkingpeer AS Peer1,
        tp3.checkedpeer AS Peer2,
        tp3.Pointsamount
    FROM transferredpoints AS tp3
    EXCEPT
    SELECT tp.id,
        tp.checkingpeer AS Peer1,
        tp.checkedpeer AS Peer2,
        tp.Pointsamount
    FROM TransferredPoints AS tp
        JOIN transferredpoints AS tp2 ON tp.checkingpeer = tp2.checkedpeer
        AND tp.checkedpeer = tp2.checkingpeer
        AND tp.id != tp2.id
),
without_checks AS (
    SELECT nrc.Peer2 AS Peer1,
        nrc.Peer1 AS Peer2,
        - tt.pointsamount AS PointsAmount
    FROM non_reciprocal_checks AS nrc
        JOIN transferredpoints AS tp4 ON nrc.id = tp4.id
        JOIN transferredpoints AS tt ON tt.checkingpeer = nrc.Peer1
        AND nrc.Peer2 = tt.checkedpeer
    UNION
    SELECT n.Peer1,
        n.Peer2,
        n.PointsAmount
    FROM non_reciprocal_checks AS n
    UNION
    SELECT mc.Peer1,
        mc.Peer2,
        mc.PointsAmount
    FROM mutual_checks AS mc
)
SELECT wc.Peer1,
    wc.Peer2,
    wc.PointsAmount
FROM without_checks AS wc
ORDER BY 1,
    2,
    3 DESC;
END;
$STATISTIC_POINTS$;

--****************************************--
--------------- TEST EX01 ------------------
--****************************************--
SELECT * FROM fnc_transferredpointsstatistic();

--------------------------------------------
-------------------- 02 --------------------
--------------------------------------------

CREATE OR REPLACE FUNCTION fnc_checks_task_xp() RETURNS TABLE(
        "Peer" VARCHAR,
        "Task" VARCHAR,
        "XP" SMALLINT
    ) LANGUAGE plpgsql AS
$AMOUNT_OF_EXPERIENCE$
BEGIN
RETURN QUERY
SELECT c.peer AS "Peer",
    split_part(c.task, '_', 1)::VARCHAR AS "Task",
    xp.xp_amount AS "XP"
FROM checks AS c
    JOIN p2p AS p ON p.check = c.id
    JOIN xp ON xp.check = c.id
WHERE p.state = 'success'
ORDER BY 1,
    2,
    3 DESC;
END;
$AMOUNT_OF_EXPERIENCE$;

--****************************************--
--------------- TEST EX02 ------------------
--****************************************--
SELECT * FROM fnc_checks_task_xp();

--------------------------------------------
-------------------- 03 --------------------
--------------------------------------------

CREATE OR REPLACE FUNCTION fnc_hardworking_peers("Day" DATE) RETURNS TABLE(
    "Peer" VARCHAR
    )
LANGUAGE plpgsql AS
$HARDWORKING_PEERS$
BEGIN
RETURN QUERY
WITH all_track AS (
    SELECT tt.peer,
        count(*) AS number_do
    FROM timetracking AS tt
    WHERE tt."Date" = "Day"
    GROUP BY tt.peer
)
SELECT peer AS "Peer"
FROM all_track
WHERE number_do = 2;
END;
$HARDWORKING_PEERS$;


--****************************************--
--------------- TEST EX03 ------------------
--****************************************--
SELECT * FROM fnc_hardworking_peers('2022-12-23');
SELECT * FROM fnc_hardworking_peers('2022-12-24');
SELECT * FROM fnc_hardworking_peers('2022-12-25');

--------------------------------------------
-------------------- 04 --------------------
--------------------------------------------

CREATE OR REPLACE FUNCTION fnc_points_traffic_all() RETURNS TABLE(
        "Peer" VARCHAR,
        "PointsChange" BIGINT
    ) LANGUAGE plpgsql AS
$POINTS_TRAFFIC_ALL_PEERS$
BEGIN
RETURN QUERY
WITH tp_checkingpeer AS (
    SELECT tp.checkingpeer AS peer1,
        sum(tp.pointsamount)::BIGINT AS sum1
    FROM transferredpoints AS tp
        JOIN peers AS p ON p.nickname = tp.checkingpeer
    GROUP BY tp.checkingpeer
    EXCEPT ALL
    SELECT tp2.checkedpeer AS peer1,
        sum(tp2.pointsamount)::BIGINT AS sum1
    FROM transferredpoints AS tp2
        JOIN peers AS p2 ON p2.nickname = tp2.checkedpeer
    GROUP BY tp2.checkedpeer
),
tp_checkedpeer AS (
    SELECT tp2.checkedpeer AS "Peer2",
        sum(tp2.pointsamount)::BIGINT AS sum2
    FROM transferredpoints AS tp2
        JOIN peers AS p2 ON p2.nickname = tp2.checkedpeer
    GROUP BY tp2.checkedpeer
    EXCEPT ALL
    SELECT tp.checkingpeer AS "Peer2",
        sum(tp.pointsamount)::BIGINT AS sum2
    FROM transferredpoints AS tp
        JOIN peers AS p ON p.nickname = tp.checkingpeer
    GROUP BY tp.checkingpeer
),
all_peers AS (
    SELECT COALESCE(tp1.peer1, p.nickname) AS checkingpeer,
        COALESCE((tp2."Peer2"), p.nickname) AS checkedpeer,
        COALESCE(sum(tp1.sum1), 0) plus_point,
        COALESCE(sum(tp2.sum2), 0) minus_point
    FROM peers AS p
        FULL JOIN tp_checkingpeer AS tp1 ON tp1.peer1 = p.nickname
        FULL JOIN tp_checkedpeer AS tp2 ON tp2."Peer2" = p.nickname
    GROUP BY tp2."Peer2",
        tp1.peer1,
        p.nickname
)
SELECT a.checkingpeer AS "Peer",
    (a.plus_point - a.minus_point)::BIGINT AS "PointsChange"
FROM all_peers AS a
ORDER BY 2 DESC;
END;
$POINTS_TRAFFIC_ALL_PEERS$;

CREATE OR REPLACE FUNCTION fnc_points_traffic() RETURNS TABLE(
        "Peer" VARCHAR,
        "PointsChange" BIGINT
    ) LANGUAGE plpgsql AS
$POINTS_TRAFFIC_PEERS$
BEGIN
RETURN QUERY
WITH tp_checkingpeer AS (
    SELECT tp.checkingpeer AS "Peer",
        SUM(tp.pointsamount) AS "PointsChange"
    FROM transferredpoints AS tp
    GROUP BY tp.checkingpeer
    ORDER BY 2 DESC
),
tp_checkedpeer AS (
    SELECT tp2.checkedpeer "Peer",
        - SUM(tp2.pointsamount) AS "PointsChange"
    FROM transferredpoints AS tp2
    GROUP BY tp2.checkedpeer
    ORDER BY 2
),
jast_peers AS (
    SELECT *
    FROM tp_checkingpeer
    UNION
    SELECT *
    FROM tp_checkedpeer
)
SELECT jp."Peer",
    SUM(jp."PointsChange")::BIGINT AS "PointsChange"
FROM jast_peers AS jp
GROUP BY jp."Peer"
ORDER BY 2 DESC;
END;
$POINTS_TRAFFIC_PEERS$;

--****************************************--
--------------- TEST EX04 ------------------
--****************************************--
SELECT * FROM fnc_points_traffic();
SELECT * FROM fnc_points_traffic_all();


--------------------------------------------
-------------------- 05 --------------------
--------------------------------------------

CREATE OR REPLACE FUNCTION fnc_point_changes() RETURNS TABLE(
        "Peer" VARCHAR,
        "PointsChange" BIGINT
    ) LANGUAGE plpgsql AS
$POINT_CHANGES$
BEGIN
RETURN QUERY
SELECT tp."Peer1" AS "Peer",
    SUM(tp."PointsAmount")::BIGINT AS "PointsAmount"
FROM fnc_TransferredPointsStatistic() AS tp
GROUP BY tp."Peer1"
ORDER BY 2 DESC;
END;
$POINT_CHANGES$;

--****************************************--
--------------- TEST EX05 ------------------
--****************************************--
SELECT * FROM fnc_point_changes();

--------------------------------------------
-------------------- 06 --------------------
--------------------------------------------
CREATE OR REPLACE PROCEDURE prc_find_max_popular_success_task_per_day(INOUT result_ REFCURSOR DEFAULT 'result_query')
    LANGUAGE plpgsql AS
$$
BEGIN
    open result_ for
    WITH agregate_tasks AS (
        SELECT count(task) as count_t,
               date,
               task
        FROM checks c
        GROUP BY date,
                 task
        ORDER BY date
    ),
         only_max_values AS (
             SELECT max(at.count_t) max_number,
                    at.date         "day"
             FROM agregate_tasks at
             GROUP BY 2
         )
    SELECT omv.day,
           split_part(at.task, '_', 1) task
    FROM only_max_values omv
             JOIN agregate_tasks at ON omv.max_number = at.count_t
        AND omv.day = at.date;
END;
$$;


--****************************************--
--------------- TEST EX06 ------------------
--****************************************--


BEGIN;
CALL prc_find_max_popular_success_task_per_day();
FETCH ALL FROM result_query;
END;

--------------------------------------------
-------------------- 07 --------------------
--------------------------------------------
-- DROP PROCEDURE prc_peers_ended_this_block();

CREATE OR REPLACE PROCEDURE prc_peers_ended_this_block(block_name VARCHAR(255), INOUT result_ REFCURSOR DEFAULT 'result_query')
    LANGUAGE plpgsql AS $FIND_PEERS_ENDED_BLOCK$
    BEGIN
        open result_ for
            WITH first_project_of_core AS (SELECT t.title FROM tasks t WHERE parent_task IS NULL),
                 last_project_from_block1 AS (SELECT t2.parent_task
                                              FROM tasks t2
                                              WHERE substring(t2.title, '[A-Z]+') !=
                                                    substring(t2.parent_task, '[A-Z]+')),
                 last_project_from_block2 AS (SELECT lpfb1.parent_task
                                              FROM last_project_from_block1 lpfb1,
                                                   first_project_of_core fpoc
                                              WHERE lpfb1.parent_task != fpoc.title),
                 union_title_plus_parrent AS (SELECT t3.title
                                              FROM tasks t3
                                              UNION ALL
                                              SELECT t4.parent_task
                                              FROM tasks t4),
                 last_project_tupic_branch AS (SELECT utpp.title, count(utpp.title) detect
                                               FROM union_title_plus_parrent utpp
                                               GROUP BY 1),
                 all_last_pojects AS (SELECT lptb.title
                                      FROM last_project_tupic_branch lptb
                                      WHERE lptb.detect = 1
                                      UNION ALL
                                      SELECT *
                                      FROM last_project_from_block2)
            SELECT c.peer,
                   c.date "day"
            FROM checks c
                     JOIN all_last_pojects alp ON alp.title = c.task
                     JOIN xp on c.id = xp."check"
            WHERE substring(c.task, '[A-Z]+') = block_name
            ORDER BY 2 DESC;
END;
    $FIND_PEERS_ENDED_BLOCK$;


--****************************************--
--------------- TEST EX07 ------------------
--****************************************--
BEGIN;
CALL prc_peers_ended_this_block('C');
FETCH ALL FROM result_query;
close result_query;
END;

-------
BEGIN;
CALL prc_peers_ended_this_block('CPP');
FETCH ALL FROM result_query;
close result_query;
END;

-------

BEGIN;
CALL prc_peers_ended_this_block('A');
FETCH ALL FROM result_query;
close result_query;
END;

--------------------------------------------
-------------------- 08 --------------------
--------------------------------------------

CREATE OR REPLACE FUNCTION fnc_recommendation_peer() RETURNS TABLE(
        "Peer" VARCHAR,
       "RecommendedPeer" VARCHAR
    ) LANGUAGE plpgsql AS
$RECOMMENDATION$
BEGIN
RETURN QUERY
WITH list_recommend AS (
    SELECT p.nickname,
        f.peer2 AS friend,
        r.recommendedpeer
    FROM peers AS p
        JOIN friends AS f ON p.nickname = f.peer1
        JOIN recommendations AS r ON r.peer = f.peer2
    WHERE r.recommendedpeer != p.nickname
    UNION ALL
    SELECT p.nickname,
        f.peer1 AS friend,
        r.recommendedpeer
    FROM peers AS p
        JOIN friends AS f ON p.nickname = f.peer2
        JOIN recommendations AS r ON r.peer = f.peer1
    WHERE r.recommendedpeer != p.nickname
),
all_recomendations AS (
    SELECT DISTINCT ON (nickname) nickname,
        recommendedpeer,
        count(*) AS number_recomendation
    FROM list_recommend
    GROUP BY nickname,
        recommendedpeer
    ORDER BY 1,
        3 DESC,
        2 DESC
)
SELECT nickname AS "Peer",
    recommendedpeer AS "RecommendedPeer" --,  number_recomendation
FROM all_recomendations;
END;
$RECOMMENDATION$;

--****************************************--
--------------- TEST EX08 ------------------
--****************************************--
SELECT * FROM fnc_recommendation_peer();

--------------------------------------------
-------------------- 09 --------------------
--------------------------------------------
-- DROP PROCEDURE prc_percentage_of_peers_blocks_started(blockA VARCHAR, blockB VARCHAR, result REFCURSOR);

CREATE OR REPLACE PROCEDURE prc_percentage_of_peers_blocks_started(blockA VARCHAR(255), blockB VARCHAR(255), INOUT result REFCURSOR DEFAULT 'result_query')
    LANGUAGE plpgsql AS $FIND_PERSENTAGE$
DECLARE
    number_of_peers BIGINT;
BEGIN
SELECT count(*) INTO number_of_peers
FROM peers;
open result for WITH all_peers_started_blocks AS (
    SELECT peer,
        substring(task, '[A-Z]+')::varchar(255) started_block
    FROM checks c
        JOIN p2p p ON c.id = p."check"
    GROUP BY 2,
        1
    ORDER BY 1
),
started_blockA AS (
    SELECT count(*) StartedBlock1
    FROM all_peers_started_blocks
    WHERE started_block = blockA
),
started_blockB AS (
    SELECT count(*) StartedBlock2
    FROM all_peers_started_blocks
    WHERE started_block = blockB
),
started_both_blocks AS (
    SELECT count(*) StartedBothBlocks
    FROM all_peers_started_blocks a1
        JOIN all_peers_started_blocks a2 ON a1.peer = a2.peer
    WHERE a1.started_block = blockA
        AND a2.started_block = blockB
),
didnt_started_blocks AS (
    SELECT count(*) DidntStartAnyBlock
    FROM all_peers_started_blocks
    WHERE started_block NOT IN (blockA, blockB)
)
SELECT (
        SELECT (StartedBlock1 * 100::numeric / number_of_peers)::REAL StartedBlock1
        FROM started_blockA
    ),
    (
        SELECT (StartedBlock2 * 100::numeric / number_of_peers)::REAL StartedBlock2
        FROM started_blockB
    ),
    (
        SELECT (
                StartedBothBlocks * 100::numeric / number_of_peers
            )::REAL StartedBothBlocks
        FROM started_both_blocks
    ),
    (
        SELECT (
                DidntStartAnyBlock * 100::numeric / number_of_peers
            )::REAL DidntStartAnyBlock
        FROM didnt_started_blocks
    );
END;
$FIND_PERSENTAGE$;

--****************************************--
--------------- TEST EX09 ------------------
--****************************************--

BEGIN;
CALL prc_percentage_of_peers_blocks_started('CPP', 'DO');
FETCH ALL FROM result_query;
END;

-- надо протащить одного пира в ветку А, и еще одного в ветку DO для 9ого задания

--------------------------------------------
-------------------- 10 --------------------
--------------------------------------------
CREATE OR REPLACE FUNCTION fnc_status_checks_procent() RETURNS TABLE(
        "SuccessfulChecks" BIGINT,
        "UnsuccessfulChecks" BIGINT
    ) LANGUAGE plpgsql AS
$CHECKS_PROCENT$
BEGIN
RETURN QUERY
WITH success_checks AS (
    SELECT COALESCE (count(p2p.state), NULL)::BIGINT AS "Success"
    FROM peers AS p
        INNER JOIN checks AS ch ON p.nickname = ch.peer
        JOIN p2p ON p2p.check = ch.id
        AND p2p.state = 'success'
        LEFT JOIN xp AS v ON v.check = ch.id
    WHERE EXTRACT(
            day
            FROM p.birthday
        ) = EXTRACT(
            day
            FROM ch."date"
        )
        AND EXTRACT(
            month
            FROM p.birthday
        ) = EXTRACT(
            month
            FROM ch."date"
        )
),
fail_checks AS (
    SELECT COALESCE(count(p2p.state), count(v.state), NULL)::BIGINT AS "Fail"
    FROM peers AS p
        INNER JOIN checks AS ch ON p.nickname = ch.peer
        AND EXTRACT(
            day
            FROM p.birthday
        ) = EXTRACT(
            day
            FROM ch."date"
        )
        AND EXTRACT(
            month
            FROM p.birthday
        ) = EXTRACT(
            month
            FROM ch."date"
        )
        JOIN p2p ON p2p.check = ch.id
        LEFT JOIN verter AS v ON v.check = ch.id
    WHERE p2p.state = 'fail'
        OR (
            p2p.state = 'success'
            AND v.state = 'fail'
        )
)
SELECT (
        GREATEST(s."Success", 0.0)::NUMERIC / GREATEST((s."Success"::NUMERIC + f."Fail"), 1.0) * 100
    )::BIGINT AS "SuccessfulChecks",
    (
        GREATEST(f."Fail", 0.0)::NUMERIC / GREATEST((s."Success"::NUMERIC + f."Fail"), 1.0) * 100
    )::BIGINT AS "UnsuccessfulChecks"
FROM fail_checks AS f
    CROSS JOIN success_checks AS s;
END;
$CHECKS_PROCENT$;

--****************************************--
--------------- TEST EX10 ------------------
--****************************************--

SELECT *  FROM fnc_status_checks_procent();

SELECT p.check, x.xp_amount
FROM p2p AS p JOIN xp AS x ON p.check = x.check
WHERE p.state = 'success'
EXCEPT
SELECT v.check, x.xp_amount
FROM verter AS v JOIN xp AS x ON v.check = x.check
WHERE v.state != 'start';


--------------------------------------------
-------------------- 11 --------------------
--------------------------------------------

CREATE OR REPLACE PROCEDURE prc_third_task_not_completed(
   firsttask VARCHAR,
   secondtask VARCHAR,
   thirdtask VARCHAR,
   IN _result_one refcursor DEFAULT 'result'
)
LANGUAGE plpgsql  AS
$$
BEGIN
OPEN _result_one for
SELECT DISTINCT ch.peer AS "Peer"
FROM checks AS ch
    JOIN xp AS x ON x.check = ch.id
WHERE ch.task = firsttask --'CPP1_s21_matrix+'
INTERSECT
SELECT DISTINCT ch.peer
FROM checks AS ch
    JOIN xp AS x ON x.check = ch.id
WHERE ch.task = secondtask --'CPP2_s21_containers'
EXCEPT
SELECT DISTINCT ch.peer
FROM checks AS ch
    JOIN xp AS x ON x.check = ch.id
WHERE ch.task = thirdtask; --'CPP3_SmartCalc_v2.0';
END;
$$;


--****************************************--
--------------- TEST EX11 ------------------
--****************************************--

BEGIN;
call prc_third_task_not_completed(
   'CPP1_s21_matrix+',
   'CPP2_s21_containers',
   'A2_SimpleNavigator v1.0'
);
FETCH ALL FROM "result";
END;


---------------
BEGIN;
CALL prc_third_task_not_completed(
   'C8_3DViewer_v1.0',
   'CPP1_s21_matrix+',
   'CPP2_s21_containers'
);
FETCH ALL FROM "result";
END;

--------------
BEGIN;
CALL prc_third_task_not_completed(
   'CPP2_s21_containers',
   'CPP3_SmartCalc_v2.0',
   'A1_MAZE'
);
FETCH ALL FROM "result";
END;

--------------------------------------------
-------------------- 12 --------------------
--------------------------------------------


CREATE OR REPLACE PROCEDURE prc_number_of_parents(INOUT result_ REFCURSOR DEFAULT 'result_query')
    LANGUAGE plpgsql AS
$$
BEGIN
    open result_ for
        WITH RECURSIVE task_parents ("taskProject", "numberPerentsTask") AS (
            SELECT t1.title AS "taskProject",
                   0        AS numberPerentsTask
            FROM tasks AS t1
            WHERE parent_task IS NULL
               OR parent_task = ''
            UNION ALL
            SELECT t2.title,
                   "numberPerentsTask" + 1 AS numberPerentsTask
            FROM task_parents AS tp
                     INNER JOIN tasks AS t2 ON tp."taskProject" = t2.parent_task
        )
        SELECT *
        FROM task_parents;
END;
$$;

---test for ex12
BEGIN;
CALL prc_number_of_parents();
FETCH ALL FROM result_query;
END;


--------------------------------------------
-------------------- 13 --------------------
--------------------------------------------
DROP FUNCTION IF EXISTS fnc_statistic_checks;
CREATE OR REPLACE FUNCTION fnc_statistic_checks() RETURNS TABLE(
        checks_id BIGINT,
        date_check DATE,
        time_begin_p2p TIME,
        status_check check_state
    ) LANGUAGE plpgsql AS
$STATISTIC_CHECKS$
BEGIN
RETURN QUERY
SELECT DISTINCT ON (p.check) p.check::BIGINT AS checks_id,
    ch."date"::DATE AS date_check,
    (
        SELECT p2p.time::TIME
        FROM p2p
        WHERE p2p.check = p.check
            AND p2p.state = 'start'
    ) AS time_begin_p2p,
    COALESCE(
        (
            SELECT (
                    CASE
                        WHEN 80.0 <= (x.xp_amount::NUMERIC / t.max_xp * 100.0)::REAL THEN 'success'
                        ELSE 'fail'
                    END
                ) as "state"
            FROM checks AS ch
                JOIN tasks AS t ON ch.task = t.title
                LEFT JOIN xp AS x ON x.check = ch.id
            WHERE ch.id = p.check
        ),
        'fail'
    )::check_state AS status_check
FROM p2p AS p
    JOIN checks AS ch ON ch.id = p.check
    JOIN tasks AS t ON ch.task = t.title
    LEFT JOIN xp AS x ON x.check = ch.id
WHERE p.state != 'start';
END;
$STATISTIC_CHECKS$;


-- Head Function

CREATE OR REPLACE PROCEDURE prc_find_good_days_for_checks
    (
    N BIGINT DEFAULT 3,
    IN _result_two refcursor DEFAULT 'result'
    ) LANGUAGE plpgsql AS
$STATISTIC$
DECLARE
    a TEXT[];
    list RECORD;
    number_s  BIGINT DEFAULT 0;
    last_date DATE DEFAULT (SELECT min(date_check) FROM fnc_statistic_checks());
    list_out TEXT DEFAULT '';
BEGIN
    for list in
        SELECT * FROM fnc_statistic_checks() ORDER BY 2,3
LOOP
    IF list.date_check != last_date  THEN
        IF number_s >= N THEN
            RAISE NOTICE '%', TO_CHAR (last_date, 'DD FMMonth yyyy (FMDAY)');
            IF list_out = '' THEN
                list_out := format('%s,', last_date);
            ELSE
                list_out := format('%s%s,',list_out, last_date);
            END IF;
        END IF;
        number_s := 0;
    END IF;
    IF list.status_check = 'success'THEN
                number_s := number_s + 1;
    ELSE
        IF number_s >= N THEN
                    RAISE NOTICE '%', TO_CHAR (list.date_check, 'DD FMMonth yyyy (FMDAY)');
            IF list_out = '' THEN
                list_out := format('%s,', list.date_check);
            ELSE
                list_out := format('%s%s,',list_out, list.date_check);
            END IF;
        END IF;
        number_s := 0;
    END IF;

    last_date := list.date_check;
END LOOP;
    IF list_out = '' THEN
    RAISE NOTICE ' NO GoodDaysForChecks';
    OPEN _result_two for
        SELECT ' NO GoodDaysForChecks' AS "GoodDaysForChecks";
    ELSE
    a := string_to_array(list_out, ',') ;
    OPEN _result_two for
        SELECT  TO_CHAR (id::DATE, 'DD FMMonth yyyy (FMDAY)') AS "GoodDaysForChecks" FROM unnest(a)AS id  WHERE id != '' ;
    END IF;
END;
$STATISTIC$;


--****************************************--
--------------- TEST EX13 ------------------
--****************************************--
-- DEFAULT value = 3 and 'result'

BEGIN;
call prc_find_good_days_for_checks(4);
FETCH ALL FROM "result";
END;

BEGIN;
call prc_find_good_days_for_checks(2);
FETCH ALL FROM "result";
END;
--
BEGIN;
call prc_find_good_days_for_checks(1);
FETCH ALL FROM "result";
END;

-- test function fnc_statistic_checks
SELECT * FROM fnc_statistic_checks() ORDER BY 2,3;
SELECT * FROM fnc_statistic_checks() WHERE date_check = '2023-02-28' ORDER BY 3;


--------------------------------------------
-------------------- 14 --------------------
--------------------------------------------
CREATE OR REPLACE FUNCTION fnc_peer_with_max_xp() RETURNS TABLE
("Peer" VARCHAR, "XP" BIGINT)
LANGUAGE plpgsql AS
$MAX_XP$
BEGIN
RETURN QUERY
WITH info_peer_xp AS (
    SELECT DISTINCT ON (ch.task, ch.peer) ch.task,
        ch."date",
        ch.peer,
        x.xp_amount,
        p.time
    FROM checks AS ch
        INNER JOIN xp AS x ON ch.id = x.check
        JOIN p2p AS p ON p.check = ch.id
        AND p.state = 'success'
    ORDER BY 1,
        3,
        2 DESC,
        5 DESC
)
SELECT peer AS "Peer",
    sum(xp_amount) AS "XP"
FROM info_peer_xp
GROUP BY peer
ORDER BY 2 DESC,
    1
LIMIT 1;
END;
$MAX_XP$;

--****************************************--
--------------- TEST EX14 ------------------
--****************************************--
SELECT * FROM fnc_peer_with_max_xp();


--------------------------------------------
-------------------- 15 --------------------
--------------------------------------------
-- DROP PROCEDURE IF EXISTS prc_determine_cames(specified_number_of_inputs BIGINT, specified_time_of_came TIME WITHOUT TIME ZONE, result REFCURSOR);

CREATE OR REPLACE PROCEDURE prc_determine_cames(
        specified_number_of_inputs BIGINT,
        specified_time_of_came TIME WITHOUT TIME ZONE,
        result REFCURSOR DEFAULT 'result_query'
    ) LANGUAGE plpgsql AS
$DETERMINE_CAMES$
BEGIN
OPEN result for
WITH peers_come_before_time AS (
        SELECT count(*) number_of_cames,
            peer
        FROM timetracking tr
        WHERE "State" = 1
            AND "Time" < specified_time_of_came
        GROUP BY 2
    )
SELECT peer nickname
FROM peers_come_before_time
WHERE number_of_cames >= specified_number_of_inputs;
END;
$DETERMINE_CAMES$;

--****************************************--
--------------- TEST EX15 ------------------
--****************************************--
BEGIN;
CALL prc_determine_cames(3, '20:00:00');
FETCH ALL FROM "result_query";
END;

BEGIN;
CALL prc_determine_cames(1, '23:00:00');
FETCH ALL FROM "result_query";
END;

BEGIN;
CALL prc_determine_cames(4, '14:00:00');
FETCH ALL FROM "result_query";
END;

--------------------------------------------
-------------------- 16 --------------------
--------------------------------------------

-- DROP PROCEDURE IF EXISTS prc_determine_lefts(specified_days_before date, specified_number_of_lefts BIGINT, result_ REFCURSOR)

CREATE OR REPLACE PROCEDURE prc_determine_lefts(specified_days_before SMALLINT, specified_number_of_lefts BIGINT,
                                                 result_ REFCURSOR DEFAULT 'result_query')
    LANGUAGE plpgsql AS
$DETERMINE_LEFTS$
BEGIN
OPEN result_ for WITH list_of_lefts AS (
    SELECT count(*) number_of_lefts,
        peer
    FROM timetracking tr
    WHERE "State" = 2
        AND tr."Date" BETWEEN (
            (
                SELECT "Date"
                FROM timetracking
                ORDER BY "Date" DESC
                LIMIT 1
            ) - specified_days_before
        )
        AND (
            SELECT "Date"
            FROM timetracking
            ORDER BY "Date" DESC
            LIMIT 1
        )
    GROUP BY tr.peer
)
SELECT lol.peer nickname
FROM list_of_lefts lol
WHERE lol.number_of_lefts >= specified_number_of_lefts;
END;
$DETERMINE_LEFTS$;

--****************************************--
--------------- TEST EX16 ------------------
--****************************************--
BEGIN;
CALL prc_determine_lefts('1', '3');
FETCH ALL FROM "result_query";
END;

BEGIN;
CALL prc_determine_lefts('1', '2');
FETCH ALL FROM "result_query";
END;

BEGIN;
CALL prc_determine_lefts('3', '2');
FETCH ALL FROM "result_query";
END;

--------------------------------------------
-------------------- 17 --------------------
--------------------------------------------

CREATE OR REPLACE PROCEDURE prc_early_entries_permonth(INOUT result_ REFCURSOR DEFAULT 'result_query') LANGUAGE plpgsql AS $$
BEGIN
    open result_ for
    WITH months_number AS (
        SELECT generate_series.generate_series number_of_month
        FROM generate_series(
                '2023.01.01'::date,
                '2023.12.01'::date,
                interval '1 month'
            )
    ),
         months_name AS (
             SELECT to_char(number_of_month, 'Month') AS mon_name,
                    number_of_month
             FROM months_number mn
         ),
         peers_and_months_of_birth AS (
             SELECT nickname,
                    to_char(birthday, 'Month') "month_of_birth"
             FROM peers
         ),
         month_of_coming AS (
             SELECT nickname,
                    pmb.month_of_birth,
                    tr."Date" date_of_come,
                    tr."Time"
             FROM peers_and_months_of_birth pmb
                      LEFT JOIN timetracking tr ON tr.peer = pmb.nickname
             WHERE tr.id IS NOT NULL
               AND tr."State" = 1
         ),
         total_entries AS (
             SELECT *,
                    (
                        SELECT count(mc.nickname)
                        FROM month_of_coming mc
                        WHERE mn2.mon_name = mc.month_of_birth
                          AND mn2.mon_name = to_char(mc.date_of_come, 'Month')
                    ) total_number_of_entries
             FROM months_name mn2
         ),
         early_entries AS (
             SELECT *,
                    (
                        SELECT count(mc.nickname)
                        FROM month_of_coming mc
                        WHERE mn2.mon_name = mc.month_of_birth
                          AND mn2.mon_name = to_char(mc.date_of_come, 'Month')
                          AND mc."Time" < '12:00:00'
                    ) number_of_early_entries
             FROM months_name mn2
         )
    SELECT te.mon_name "Month",
           CASE
               te.total_number_of_entries
               WHEN 0 THEN 0
               ELSE ee.number_of_early_entries / te.total_number_of_entries * 100::real
               END     "EarlyEntries"
    FROM total_entries te,
         early_entries ee
    WHERE te.mon_name = ee.mon_name
    ORDER BY te.number_of_month;
END;
$$;


---- test for ex17

BEGIN;
CALL prc_early_entries_permonth();
FETCH ALL FROM result_query;
END;
