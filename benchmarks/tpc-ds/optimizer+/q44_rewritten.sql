WITH A2 AS (
    SELECT
        A3.C0 C0,
        (A3.C1 / A3.C2) C1
    FROM
        (
            SELECT
                A4.SS_ITEM_SK C0,
                SUM(A4.SS_NET_PROFIT) C1,
                COUNT(A4.SS_NET_PROFIT) C2
            FROM
                STORE_SALES A4
            WHERE
                (A4.SS_STORE_SK = 6)
            GROUP BY
                A4.SS_ITEM_SK
        ) A3
    WHERE
        (
            (
                0.9 * (
                    SELECT
                        (A5.C0 / A5.C1)
                    FROM
                        (
                            SELECT
                                SUM(A6.SS_NET_PROFIT) C0,
                                COUNT(A6.SS_NET_PROFIT) C1
                            FROM
                                STORE_SALES A6
                            WHERE
                                (A6.SS_STORE_SK = 6)
                                AND (A6.SS_HDEMO_SK IS NULL)
                            GROUP BY
                                A6.SS_STORE_SK
                        ) A5
                )
            ) < (A3.C1 / A3.C2)
        )
)
SELECT
    A1.C1 "RNK",
    A0.I_PRODUCT_NAME "BEST_PERFORMING",
    A8.I_PRODUCT_NAME "WORST_PERFORMING"
FROM
    (
        (
            ITEM A0
            INNER JOIN (
                SELECT
                    "A7".C0 C0,
                    RANK() OVER(
                        ORDER BY
                            "A7".C1 ASC
                    ) C1
                FROM
                    A2 "A7"
            ) A1 ON (A0.I_ITEM_SK = A1.C0)
        )
        INNER JOIN (
            ITEM A8
            INNER JOIN (
                SELECT
                    "A10".C0 C0,
                    RANK() OVER(
                        ORDER BY
                            "A10".C1 DESC
                    ) C1
                FROM
                    A2 "A10"
            ) A9 ON (A8.I_ITEM_SK = A9.C0)
        ) ON (A1.C1 = A9.C1)
    )
WHERE
    (A1.C1 < 11)
    AND (A9.C1 < 11)
ORDER BY
    1 ASC
limit
    100;