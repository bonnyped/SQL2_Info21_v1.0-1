
DROP FUNCTION IF EXISTS fnc_TransferredPointsStatistic;
DROP FUNCTION IF EXISTS fnc_checks_task_xp;
DROP FUNCTION IF EXISTS fnc_hardworking_peers;
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
            CASE
                WHEN tp2.Pointsamount > tp.Pointsamount THEN (tp.Pointsamount - tp2.Pointsamount)
                ELSE tp.Pointsamount
            END AS Pointsamount
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
            0 AS PointsAmount
        FROM non_reciprocal_checks AS nrc
            JOIN transferredpoints AS tp4 ON nrc.id = tp4.id
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
    c.task AS "Task",
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

