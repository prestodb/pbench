SELECT
    A0.C0 "I_CATEGORY",
    A0.C1 "I_CLASS",
    A0.C2 "I_BRAND",
    A0.C3 "I_PRODUCT_NAME",
    A0.C4 "D_YEAR",
    A0.C5 "D_QOY",
    A0.C6 "D_MOY",
    A0.C7 "S_STORE_ID",
    A0.C8 "SUMSALES",
    A0.C9 "RK"
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            A1.C3 C3,
            A1.C4 C4,
            A1.C5 C5,
            A1.C6 C6,
            A1.C7 C7,
            A1.C8 C8,
            RANK() OVER(
                PARTITION BY A1.C0
                ORDER BY
                    A1.C8 DESC
            ) C9
        FROM
            (
                (
                    SELECT
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C0
                            WHEN (A7.C0 = 2) THEN A2.C0
                            WHEN (A7.C0 = 3) THEN A2.C0
                            WHEN (A7.C0 = 4) THEN A2.C0
                            WHEN (A7.C0 = 5) THEN A2.C0
                            WHEN (A7.C0 = 6) THEN A2.C0
                            WHEN (A7.C0 = 7) THEN A2.C0
                            WHEN (A7.C0 = 8) THEN A2.C0
                            ELSE NULL
                        END C0,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C1
                            WHEN (A7.C0 = 2) THEN A2.C1
                            WHEN (A7.C0 = 3) THEN A2.C1
                            WHEN (A7.C0 = 4) THEN A2.C1
                            WHEN (A7.C0 = 5) THEN A2.C1
                            WHEN (A7.C0 = 6) THEN A2.C1
                            WHEN (A7.C0 = 7) THEN A2.C1
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END C1,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C2
                            WHEN (A7.C0 = 2) THEN A2.C2
                            WHEN (A7.C0 = 3) THEN A2.C2
                            WHEN (A7.C0 = 4) THEN A2.C2
                            WHEN (A7.C0 = 5) THEN A2.C2
                            WHEN (A7.C0 = 6) THEN A2.C2
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END C2,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C3
                            WHEN (A7.C0 = 2) THEN A2.C3
                            WHEN (A7.C0 = 3) THEN A2.C3
                            WHEN (A7.C0 = 4) THEN A2.C3
                            WHEN (A7.C0 = 5) THEN A2.C3
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END C3,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C4
                            WHEN (A7.C0 = 2) THEN A2.C4
                            WHEN (A7.C0 = 3) THEN A2.C4
                            WHEN (A7.C0 = 4) THEN A2.C4
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END C4,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C5
                            WHEN (A7.C0 = 2) THEN A2.C5
                            WHEN (A7.C0 = 3) THEN A2.C5
                            WHEN (A7.C0 = 4) THEN NULL
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END C5,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C6
                            WHEN (A7.C0 = 2) THEN A2.C6
                            WHEN (A7.C0 = 3) THEN NULL
                            WHEN (A7.C0 = 4) THEN NULL
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END C6,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C7
                            WHEN (A7.C0 = 2) THEN NULL
                            WHEN (A7.C0 = 3) THEN NULL
                            WHEN (A7.C0 = 4) THEN NULL
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END C7,
                        SUM(A2.C8) C8,
                        A7.C0 C9
                    FROM
                        (
                            (
                                SELECT
                                    A5.I_CATEGORY C0,
                                    A5.I_CLASS C1,
                                    A5.I_BRAND C2,
                                    A5.I_PRODUCT_NAME C3,
                                    A4.D_YEAR C4,
                                    A4.D_QOY C5,
                                    A4.D_MOY C6,
                                    A6.S_STORE_ID C7,
                                    SUM(
                                        COALESCE(
                                            (A3.SS_SALES_PRICE * A3.SS_QUANTITY),
                                            0000000000000000.00
                                        )
                                    ) C8
                                FROM
                                    (
                                        (
                                            (
                                                STORE_SALES A3
                                                INNER JOIN DATE_DIM A4 ON (A3.SS_SOLD_DATE_SK = A4.D_DATE_SK)
                                            )
                                            INNER JOIN ITEM A5 ON (A3.SS_ITEM_SK = A5.I_ITEM_SK)
                                        )
                                        INNER JOIN STORE A6 ON (A3.SS_STORE_SK = A6.S_STORE_SK)
                                    )
                                WHERE
                                    (1194 <= A4.D_MONTH_SEQ)
                                    AND (A4.D_MONTH_SEQ <= 1205)
                                GROUP BY
                                    A5.I_CATEGORY,
                                    A5.I_CLASS,
                                    A5.I_BRAND,
                                    A5.I_PRODUCT_NAME,
                                    A4.D_YEAR,
                                    A4.D_QOY,
                                    A4.D_MOY,
                                    A6.S_STORE_ID
                            ) A2
                            INNER JOIN (
                                VALUES
                                    1,
                                    2,
                                    3,
                                    4,
                                    5,
                                    6,
                                    7,
                                    8,
                                    9
                            ) A7 (C0) ON (
                                MOD(LENGTH(A7.C0), 1) = COALESCE(MOD(LENGTH(A2.C0), 1), 0)
                            )
                        )
                    GROUP BY
                        A7.C0,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C0
                            WHEN (A7.C0 = 2) THEN A2.C0
                            WHEN (A7.C0 = 3) THEN A2.C0
                            WHEN (A7.C0 = 4) THEN A2.C0
                            WHEN (A7.C0 = 5) THEN A2.C0
                            WHEN (A7.C0 = 6) THEN A2.C0
                            WHEN (A7.C0 = 7) THEN A2.C0
                            WHEN (A7.C0 = 8) THEN A2.C0
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C1
                            WHEN (A7.C0 = 2) THEN A2.C1
                            WHEN (A7.C0 = 3) THEN A2.C1
                            WHEN (A7.C0 = 4) THEN A2.C1
                            WHEN (A7.C0 = 5) THEN A2.C1
                            WHEN (A7.C0 = 6) THEN A2.C1
                            WHEN (A7.C0 = 7) THEN A2.C1
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C2
                            WHEN (A7.C0 = 2) THEN A2.C2
                            WHEN (A7.C0 = 3) THEN A2.C2
                            WHEN (A7.C0 = 4) THEN A2.C2
                            WHEN (A7.C0 = 5) THEN A2.C2
                            WHEN (A7.C0 = 6) THEN A2.C2
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C3
                            WHEN (A7.C0 = 2) THEN A2.C3
                            WHEN (A7.C0 = 3) THEN A2.C3
                            WHEN (A7.C0 = 4) THEN A2.C3
                            WHEN (A7.C0 = 5) THEN A2.C3
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C4
                            WHEN (A7.C0 = 2) THEN A2.C4
                            WHEN (A7.C0 = 3) THEN A2.C4
                            WHEN (A7.C0 = 4) THEN A2.C4
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C5
                            WHEN (A7.C0 = 2) THEN A2.C5
                            WHEN (A7.C0 = 3) THEN A2.C5
                            WHEN (A7.C0 = 4) THEN NULL
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C6
                            WHEN (A7.C0 = 2) THEN A2.C6
                            WHEN (A7.C0 = 3) THEN NULL
                            WHEN (A7.C0 = 4) THEN NULL
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A7.C0 = 1) THEN A2.C7
                            WHEN (A7.C0 = 2) THEN NULL
                            WHEN (A7.C0 = 3) THEN NULL
                            WHEN (A7.C0 = 4) THEN NULL
                            WHEN (A7.C0 = 5) THEN NULL
                            WHEN (A7.C0 = 6) THEN NULL
                            WHEN (A7.C0 = 7) THEN NULL
                            WHEN (A7.C0 = 8) THEN NULL
                            ELSE NULL
                        END
                ) A1
                RIGHT OUTER JOIN (
                    VALUES
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9
                ) A8 (C0) ON (A8.C0 = A1.C9)
            )
        WHERE
            (
                (
                    (A8.C0 = 9)
                    AND (A1.C9 IS NULL)
                )
                OR (A1.C9 IS NOT NULL)
            )
    ) A0
WHERE
    (A0.C9 <= 100)
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC,
    4 ASC,
    5 ASC,
    6 ASC,
    7 ASC,
    8 ASC,
    9 ASC,
    10 ASC
limit
    100;