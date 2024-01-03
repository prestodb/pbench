SELECT
    COUNT(*)
FROM
    (
        (
            (
                STORE_SALES A0
                INNER JOIN TIME_DIM A1 ON (A0.SS_SOLD_TIME_SK = A1.T_TIME_SK)
            )
            INNER JOIN HOUSEHOLD_DEMOGRAPHICS A2 ON (A0.SS_HDEMO_SK = A2.HD_DEMO_SK)
        )
        INNER JOIN STORE A3 ON (A0.SS_STORE_SK = A3.S_STORE_SK)
    )
WHERE
    (A1.T_HOUR = 8)
    AND (30 <= A1.T_MINUTE)
    AND (A2.HD_DEP_COUNT = 5)
    AND (A3.S_STORE_NAME = 'ese')
ORDER BY
    1 ASC
limit
    100;