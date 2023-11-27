
DROP FUNCTION IF EXISTS fnc_TransferredPointsStatistic;
DROP FUNCTION IF EXISTS fnc_checks_task_xp;
DROP FUNCTION IF EXISTS fnc_hardworking_peers;
DROP FUNCTION IF EXISTS  fnc_points_traffic_all;
DROP FUNCTION IF EXISTS  fnc_points_traffic;
DROP FUNCTION IF EXISTS fnc_point_changes;

DROP FUNCTION IF EXISTS fnc_recommendation_peer;

DROP FUNCTION IF EXISTS fnc_status_checks_procent;
DROP PROCEDURE IF EXISTS proc_third_task_not_completed(
   firsttask VARCHAR,
   secondtask VARCHAR,
   thirdtask VARCHAR,
   IN _result_one refcursor
);


----------- 01 -----------

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

-- test 01
SELECT * FROM fnc_transferredpointsstatistic();

----------- 02 -----------
CREATE OR REPLACE FUNCTION fnc_checks_task_xp() RETURNS TABLE(
        "Peer" VARCHAR,
        "Task" VARCHAR,
        "XP" SMALLINT
    ) LANGUAGE plpgsql AS 
$AMOUNT_OF_EXPERIENCE$
BEGIN 
RETURN QUERY
SELECT c.peer AS "Peer",
    split_part(c.task, '_',1)::VARCHAR AS "Task",
    xp.xp_amount AS "XP"
FROM checks AS c
    JOIN p2p AS p ON p.check = c.id
    JOIN verter AS v ON v.check = c.id
    JOIN xp ON xp.check = c.id
WHERE p.state = 'success'
    AND v.state = 'success'
    ORDER BY 1,2, 3 DESC;
END;
$AMOUNT_OF_EXPERIENCE$;

-- test 02
SELECT * FROM fnc_checks_task_xp();

----------- 03 -----------
CREATE OR REPLACE FUNCTION fnc_hardworking_peers("Day" DATE) RETURNS TABLE(
        "Peer" VARCHAR
    ) LANGUAGE plpgsql AS 
$HARDWORKING_PEERS$
BEGIN 
RETURN QUERY
SELECT  DISTINCT tt.peer
FROM timetracking AS tt WHERE tt."Date" = "Day" AND tt."State" = 1
GROUP BY tt."State" , tt.peer
EXCEPT
SELECT  DISTINCT tt.peer
FROM timetracking AS tt WHERE tt."Date" = "Day" AND tt."State" = 2
GROUP BY tt."State" , tt.peer;
END;
$HARDWORKING_PEERS$;


-- test 03
SELECT * FROM fnc_hardworking_peers('2022-12-24');

----------- 04 -----------

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
            GROUP BY tp2."Peer2", tp1.peer1, p.nickname
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

-- tests 04
SELECT * FROM fnc_points_traffic();
SELECT * FROM fnc_points_traffic_all();


----------- 05 -----------

CREATE OR REPLACE FUNCTION fnc_point_changes() RETURNS TABLE(
        "Peer" VARCHAR,
        "PointsChange" BIGINT
    ) LANGUAGE plpgsql AS 
$POINT_CHANGES$ 
BEGIN 
RETURN QUERY
    SELECT tp."Peer1" AS "Peer",
        SUM( tp."PointsAmount")::BIGINT AS "PointsAmount"
    FROM fnc_TransferredPointsStatistic() AS tp
    GROUP BY tp."Peer1"
    ORDER BY 2 DESC;
END;
$POINT_CHANGES$;

-- test 05 
SELECT * FROM fnc_point_changes();

----------- 06 -----------

-- test 06

----------- 07 -----------

-- test 07


----------- 08 -----------

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

-- test 08
SELECT * FROM fnc_recommendation_peer();



----------- 10 -----------
CREATE OR REPLACE FUNCTION fnc_status_checks_procent() RETURNS TABLE(
        "SuccessfulChecks" BIGINT,
        "UnsuccessfulChecks" BIGINT
    ) LANGUAGE plpgsql AS
$CHECKS_PROCENT$ 
BEGIN
RETURN QUERY
WITH success_checks AS (
        SELECT COALESCE (count(v.state), NULL)::BIGINT AS "Success"
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
        WHERE v.state = 'success'
            AND p2p.state = 'success'
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
--test 10
SELECT *  FROM fnc_status_checks_procent();


----------- 11 -----------

CREATE OR REPLACE PROCEDURE proc_third_task_not_completed(
   firsttask VARCHAR,
   secondtask VARCHAR,
   thirdtask VARCHAR,
   IN _result_one refcursor DEFAULT 'result'
) 
LANGUAGE plpgsql  AS  
$$
begin
open _result_one for
    SELECT DISTINCT ch.peer AS "Peer"
    FROM checks AS ch JOIN verter AS v ON v.check = ch.id AND v.state = 'success'
    WHERE ch.task = firsttask --'CPP1_s21_matrix+'
    INTERSECT
    SELECT DISTINCT ch.peer
    FROM checks AS ch JOIN verter AS v ON v.check = ch.id AND v.state = 'success'
    WHERE ch.task = secondtask --'CPP2_s21_containers'
    EXCEPT
    SELECT DISTINCT ch.peer
    FROM checks AS ch JOIN verter AS v ON v.check = ch.id AND v.state = 'success'
    WHERE ch.task = thirdtask; --'CPP3_SmartCalc_v2.0';
    
end;
$$;


-- test 11
BEGIN;
call proc_third_task_not_completed(
   'CPP1_s21_matrix+',
   'CPP2_s21_containers',
   'CPP3_SmartCalc_v2.0'
);
FETCH ALL FROM "result";
END;



-- CREATE OR REPLACE PROCEDURE proc_third_task_not_completed2(
--    firsttask VARCHAR,
--    secondtask VARCHAR,
--    thirdtask VARCHAR
-- ) 
-- LANGUAGE plpgsql  AS  
-- $$
-- DECLARE res record;
-- peersing VARCHAR(50);
-- begin
--     FOR res in SELECT ch.peer 
--     FROM checks AS ch 
--     JOIN verter AS v ON v.check = ch.id AND v.state = 'success'
--     WHERE ch.task = firsttask --'CPP1_s21_matrix+'
--     INTERSECT
--     SELECT DISTINCT ch.peer
--     FROM checks AS ch JOIN verter AS v ON v.check = ch.id AND v.state = 'success'
--     WHERE ch.task = secondtask --'CPP2_s21_containers'
--     EXCEPT
--     SELECT DISTINCT ch.peer
--     FROM checks AS ch JOIN verter AS v ON v.check = ch.id AND v.state = 'success'
--     WHERE ch.task = thirdtask LOOP --'CPP3_SmartCalc_v2.0';
--     peersing = res.peer;
--     RAISE NOTICE 'col1: %', quote_ident(res.peer);
--     END LOOP;
-- end;
-- $$;

-- call proc_third_task_not_completed2(   'CPP1_s21_matrix+',
--    'CPP2_s21_containers',
--    'CPP3_SmartCalc_v2.0');