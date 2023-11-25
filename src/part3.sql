
----------- 01 -----------
DROP FUNCTION IF EXISTS st_TransferredPoints;

CREATE OR REPLACE FUNCTION st_TransferredPoints() RETURNS TABLE(
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
        SELECT nrc.Peer2,
            nrc.Peer1,
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
SELECT * FROM st_TransferredPoints();

----------- 02 -----------