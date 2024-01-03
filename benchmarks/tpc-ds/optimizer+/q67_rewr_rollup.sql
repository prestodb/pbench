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
                            WHEN (A6.C0 = 1) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 2) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 3) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 4) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 5) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 6) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 7) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 8) THEN A5.I_CATEGORY
                            ELSE NULL
                        END C0,
                        CASE
                            WHEN (A6.C0 = 1) THEN A5.I_CLASS
                            WHEN (A6.C0 = 2) THEN A5.I_CLASS
                            WHEN (A6.C0 = 3) THEN A5.I_CLASS
                            WHEN (A6.C0 = 4) THEN A5.I_CLASS
                            WHEN (A6.C0 = 5) THEN A5.I_CLASS
                            WHEN (A6.C0 = 6) THEN A5.I_CLASS
                            WHEN (A6.C0 = 7) THEN A5.I_CLASS
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END C1,
                        CASE
                            WHEN (A6.C0 = 1) THEN A5.I_BRAND
                            WHEN (A6.C0 = 2) THEN A5.I_BRAND
                            WHEN (A6.C0 = 3) THEN A5.I_BRAND
                            WHEN (A6.C0 = 4) THEN A5.I_BRAND
                            WHEN (A6.C0 = 5) THEN A5.I_BRAND
                            WHEN (A6.C0 = 6) THEN A5.I_BRAND
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END C2,
                        CASE
                            WHEN (A6.C0 = 1) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 2) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 3) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 4) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 5) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END C3,
                        CASE
                            WHEN (A6.C0 = 1) THEN A3.D_YEAR
                            WHEN (A6.C0 = 2) THEN A3.D_YEAR
                            WHEN (A6.C0 = 3) THEN A3.D_YEAR
                            WHEN (A6.C0 = 4) THEN A3.D_YEAR
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END C4,
                        CASE
                            WHEN (A6.C0 = 1) THEN A3.D_QOY
                            WHEN (A6.C0 = 2) THEN A3.D_QOY
                            WHEN (A6.C0 = 3) THEN A3.D_QOY
                            WHEN (A6.C0 = 4) THEN NULL
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END C5,
                        CASE
                            WHEN (A6.C0 = 1) THEN A3.D_MOY
                            WHEN (A6.C0 = 2) THEN A3.D_MOY
                            WHEN (A6.C0 = 3) THEN NULL
                            WHEN (A6.C0 = 4) THEN NULL
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END C6,
                        CASE
                            WHEN (A6.C0 = 1) THEN A4.S_STORE_ID
                            WHEN (A6.C0 = 2) THEN NULL
                            WHEN (A6.C0 = 3) THEN NULL
                            WHEN (A6.C0 = 4) THEN NULL
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END C7,
                        SUM(
                            COALESCE(
                                (A2.SS_SALES_PRICE * A2.SS_QUANTITY),
                                0000000000000000.00
                            )
                        ) C8,
                        A6.C0 C9
                    FROM
                        (
                            (
                                (
                                    STORE_SALES A2
                                    INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                                )
                                INNER JOIN STORE A4 ON (A2.SS_STORE_SK = A4.S_STORE_SK)
                            )
                            INNER JOIN (
                                ITEM A5
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
                                ) A6 (C0) ON (
                                    MOD(LENGTH(A6.C0), 1) = COALESCE(MOD(LENGTH(A5.I_CATEGORY), 1), 0)
                                )
                            ) ON (A2.SS_ITEM_SK = A5.I_ITEM_SK)
                        )
                    WHERE
                        (A3.D_MONTH_SEQ <= 1205)
                        AND (1194 <= A3.D_MONTH_SEQ)
                    GROUP BY
                        A6.C0,
                        CASE
                            WHEN (A6.C0 = 1) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 2) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 3) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 4) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 5) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 6) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 7) THEN A5.I_CATEGORY
                            WHEN (A6.C0 = 8) THEN A5.I_CATEGORY
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A6.C0 = 1) THEN A5.I_CLASS
                            WHEN (A6.C0 = 2) THEN A5.I_CLASS
                            WHEN (A6.C0 = 3) THEN A5.I_CLASS
                            WHEN (A6.C0 = 4) THEN A5.I_CLASS
                            WHEN (A6.C0 = 5) THEN A5.I_CLASS
                            WHEN (A6.C0 = 6) THEN A5.I_CLASS
                            WHEN (A6.C0 = 7) THEN A5.I_CLASS
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A6.C0 = 1) THEN A5.I_BRAND
                            WHEN (A6.C0 = 2) THEN A5.I_BRAND
                            WHEN (A6.C0 = 3) THEN A5.I_BRAND
                            WHEN (A6.C0 = 4) THEN A5.I_BRAND
                            WHEN (A6.C0 = 5) THEN A5.I_BRAND
                            WHEN (A6.C0 = 6) THEN A5.I_BRAND
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A6.C0 = 1) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 2) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 3) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 4) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 5) THEN A5.I_PRODUCT_NAME
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A6.C0 = 1) THEN A3.D_YEAR
                            WHEN (A6.C0 = 2) THEN A3.D_YEAR
                            WHEN (A6.C0 = 3) THEN A3.D_YEAR
                            WHEN (A6.C0 = 4) THEN A3.D_YEAR
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A6.C0 = 1) THEN A3.D_QOY
                            WHEN (A6.C0 = 2) THEN A3.D_QOY
                            WHEN (A6.C0 = 3) THEN A3.D_QOY
                            WHEN (A6.C0 = 4) THEN NULL
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A6.C0 = 1) THEN A3.D_MOY
                            WHEN (A6.C0 = 2) THEN A3.D_MOY
                            WHEN (A6.C0 = 3) THEN NULL
                            WHEN (A6.C0 = 4) THEN NULL
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
                            ELSE NULL
                        END,
                        CASE
                            WHEN (A6.C0 = 1) THEN A4.S_STORE_ID
                            WHEN (A6.C0 = 2) THEN NULL
                            WHEN (A6.C0 = 3) THEN NULL
                            WHEN (A6.C0 = 4) THEN NULL
                            WHEN (A6.C0 = 5) THEN NULL
                            WHEN (A6.C0 = 6) THEN NULL
                            WHEN (A6.C0 = 7) THEN NULL
                            WHEN (A6.C0 = 8) THEN NULL
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
                ) A7 (C0) ON (A7.C0 = A1.C9)
            )
        WHERE
            (
                (
                    (A7.C0 = 9)
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