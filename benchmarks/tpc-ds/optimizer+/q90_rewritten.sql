SELECT
    (
        CAST(A0.C0 AS DECIMAL(15, 4)) / CAST(A5.C0 AS DECIMAL(15, 4))
    ) "AM_PM_RATIO"
FROM
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        WEB_SALES A1
                        INNER JOIN WEB_PAGE A2 ON (A1.WS_WEB_PAGE_SK = A2.WP_WEB_PAGE_SK)
                    )
                    INNER JOIN TIME_DIM A3 ON (A1.WS_SOLD_TIME_SK = A3.T_TIME_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A4 ON (A1.WS_SHIP_HDEMO_SK = A4.HD_DEMO_SK)
            )
        WHERE
            (5000 <= A2.WP_CHAR_COUNT)
            AND (A2.WP_CHAR_COUNT <= 5200)
            AND (9 <= A3.T_HOUR)
            AND (A3.T_HOUR <= 10)
            AND (A4.HD_DEP_COUNT = 2)
    ) A0,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        WEB_SALES A6
                        INNER JOIN WEB_PAGE A7 ON (A6.WS_WEB_PAGE_SK = A7.WP_WEB_PAGE_SK)
                    )
                    INNER JOIN TIME_DIM A8 ON (A6.WS_SOLD_TIME_SK = A8.T_TIME_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A9 ON (A6.WS_SHIP_HDEMO_SK = A9.HD_DEMO_SK)
            )
        WHERE
            (5000 <= A7.WP_CHAR_COUNT)
            AND (A7.WP_CHAR_COUNT <= 5200)
            AND (15 <= A8.T_HOUR)
            AND (A8.T_HOUR <= 16)
            AND (A9.HD_DEP_COUNT = 2)
    ) A5
ORDER BY
    1 ASC
limit
    100;