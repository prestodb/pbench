SELECT
    A0.C0 "ITEM_SK",
    A0.C1 "D_DATE",
    A0.C2 "WEB_SALES",
    A0.C3 "STORE_SALES",
    A0.C4 "WEB_CUMULATIVE",
    A0.C5 "STORE_CUMULATIVE"
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            A1.C3 C3,
            MAX(A1.C2) OVER(
                PARTITION BY A1.C0
                ORDER BY
                    A1.C1 ASC
            ) C4,
            MAX(A1.C3) OVER(
                PARTITION BY A1.C0
                ORDER BY
                    A1.C1 ASC
            ) C5
        FROM
            (
                SELECT
                    CASE
                        WHEN (A6.C0 IS NOT NULL) THEN A6.C0
                        ELSE A2.C0
                    END C0,
                    CASE
                        WHEN (A6.C1 IS NOT NULL) THEN A6.C1
                        ELSE A2.C1
                    END C1,
                    A6.C2 C2,
                    A2.C2 C3
                FROM
                    (
                        (
                            SELECT
                                A3.C0 C0,
                                A3.C1 C1,
                                SUM(A3.C2) OVER(
                                    PARTITION BY A3.C0
                                    ORDER BY
                                        A3.C1 ASC
                                ) C2
                            FROM
                                (
                                    SELECT
                                        A4.SS_ITEM_SK C0,
                                        A5.D_DATE C1,
                                        SUM(A4.SS_SALES_PRICE) C2
                                    FROM
                                        (
                                            STORE_SALES A4
                                            INNER JOIN DATE_DIM A5 ON (A4.SS_SOLD_DATE_SK = A5.D_DATE_SK)
                                        )
                                    WHERE
                                        (1215 <= A5.D_MONTH_SEQ)
                                        AND (A5.D_MONTH_SEQ <= 1226)
                                    GROUP BY
                                        A4.SS_ITEM_SK,
                                        A5.D_DATE
                                ) A3
                        ) A2
                        LEFT OUTER JOIN (
                            SELECT
                                A7.C0 C0,
                                A7.C1 C1,
                                SUM(A7.C2) OVER(
                                    PARTITION BY A7.C0
                                    ORDER BY
                                        A7.C1 ASC
                                ) C2
                            FROM
                                (
                                    SELECT
                                        A8.WS_ITEM_SK C0,
                                        A9.D_DATE C1,
                                        SUM(A8.WS_SALES_PRICE) C2
                                    FROM
                                        (
                                            WEB_SALES A8
                                            INNER JOIN DATE_DIM A9 ON (A8.WS_SOLD_DATE_SK = A9.D_DATE_SK)
                                        )
                                    WHERE
                                        (1215 <= A9.D_MONTH_SEQ)
                                        AND (A9.D_MONTH_SEQ <= 1226)
                                    GROUP BY
                                        A8.WS_ITEM_SK,
                                        A9.D_DATE
                                ) A7
                        ) A6 ON (A6.C0 = A2.C0)
                        AND (A6.C1 = A2.C1)
                    )
            ) A1
    ) A0
WHERE
    (A0.C5 < A0.C4)
ORDER BY
    1 ASC,
    2 ASC
limit
    100;