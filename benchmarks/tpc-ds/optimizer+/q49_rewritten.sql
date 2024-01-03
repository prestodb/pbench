SELECT
    DISTINCT A0.C0 "CHANNEL",
    A0.C1 "ITEM",
    A0.C2 "RETURN_RATIO",
    A0.C3 "RETURN_RANK",
    A0.C4 "CURRENCY_RANK"
FROM
    (
        (
            SELECT
                CAST('web' AS VARCHAR(7)) C0,
                A1.C0 C1,
                A1.C1 C2,
                A1.C2 C3,
                A1.C3 C4
            FROM
                (
                    SELECT
                        A2.C0 C0,
                        A2.C1 C1,
                        RANK() OVER(
                            ORDER BY
                                A2.C1 ASC
                        ) C2,
                        A2.C2 C3
                    FROM
                        (
                            SELECT
                                A3.C0 C0,
                                A3.C1 C1,
                                RANK() OVER(
                                    ORDER BY
                                        A3.C2 ASC
                                ) C2
                            FROM
                                (
                                    SELECT
                                        A4.C0 C0,
                                        (
                                            CAST(A4.C1 AS DECIMAL(15, 4)) / CAST(A4.C2 AS DECIMAL(15, 4))
                                        ) C1,
                                        (
                                            CAST(A4.C3 AS DECIMAL(15, 4)) / CAST(A4.C4 AS DECIMAL(15, 4))
                                        ) C2
                                    FROM
                                        (
                                            SELECT
                                                A6.WS_ITEM_SK C0,
                                                SUM(COALESCE(A5.WR_RETURN_QUANTITY, 0)) C1,
                                                SUM(COALESCE(A6.WS_QUANTITY, 0)) C2,
                                                SUM(
                                                    COALESCE(
                                                        CAST(A5.WR_RETURN_AMT AS DECIMAL(13, 2)),
                                                        00000000000.00
                                                    )
                                                ) C3,
                                                SUM(
                                                    COALESCE(
                                                        CAST(A6.WS_NET_PAID AS DECIMAL(13, 2)),
                                                        00000000000.00
                                                    )
                                                ) C4
                                            FROM
                                                (
                                                    WEB_RETURNS A5
                                                    INNER JOIN (
                                                        WEB_SALES A6
                                                        INNER JOIN DATE_DIM A7 ON (A7.D_DATE_SK = A6.WS_SOLD_DATE_SK)
                                                    ) ON (A6.WS_ITEM_SK = A5.WR_ITEM_SK)
                                                    AND (A6.WS_ORDER_NUMBER = A5.WR_ORDER_NUMBER)
                                                )
                                            WHERE
                                                (10000 < A5.WR_RETURN_AMT)
                                                AND (1 < A6.WS_NET_PROFIT)
                                                AND (0 < A6.WS_NET_PAID)
                                                AND (0 < A6.WS_QUANTITY)
                                                AND (2000 = A7.D_YEAR)
                                                AND (12 = A7.D_MOY)
                                            GROUP BY
                                                A6.WS_ITEM_SK
                                        ) A4
                                ) A3
                        ) A2
                ) A1
            WHERE
                (
                    (A1.C2 <= 10)
                    OR (A1.C3 <= 10)
                )
        )
        UNION
        ALL (
            SELECT
                'catalog' C0,
                A8.C0 C1,
                A8.C1 C2,
                A8.C2 C3,
                A8.C3 C4
            FROM
                (
                    SELECT
                        A9.C0 C0,
                        A9.C1 C1,
                        RANK() OVER(
                            ORDER BY
                                A9.C1 ASC
                        ) C2,
                        A9.C2 C3
                    FROM
                        (
                            SELECT
                                A10.C0 C0,
                                A10.C1 C1,
                                RANK() OVER(
                                    ORDER BY
                                        A10.C2 ASC
                                ) C2
                            FROM
                                (
                                    SELECT
                                        A11.C0 C0,
                                        (
                                            CAST(A11.C1 AS DECIMAL(15, 4)) / CAST(A11.C2 AS DECIMAL(15, 4))
                                        ) C1,
                                        (
                                            CAST(A11.C3 AS DECIMAL(15, 4)) / CAST(A11.C4 AS DECIMAL(15, 4))
                                        ) C2
                                    FROM
                                        (
                                            SELECT
                                                A13.CS_ITEM_SK C0,
                                                SUM(COALESCE(A12.CR_RETURN_QUANTITY, 0)) C1,
                                                SUM(COALESCE(A13.CS_QUANTITY, 0)) C2,
                                                SUM(
                                                    COALESCE(
                                                        CAST(A12.CR_RETURN_AMOUNT AS DECIMAL(13, 2)),
                                                        00000000000.00
                                                    )
                                                ) C3,
                                                SUM(
                                                    COALESCE(
                                                        CAST(A13.CS_NET_PAID AS DECIMAL(13, 2)),
                                                        00000000000.00
                                                    )
                                                ) C4
                                            FROM
                                                (
                                                    CATALOG_RETURNS A12
                                                    INNER JOIN (
                                                        CATALOG_SALES A13
                                                        INNER JOIN DATE_DIM A14 ON (A14.D_DATE_SK = A13.CS_SOLD_DATE_SK)
                                                    ) ON (A13.CS_ITEM_SK = A12.CR_ITEM_SK)
                                                    AND (A13.CS_ORDER_NUMBER = A12.CR_ORDER_NUMBER)
                                                )
                                            WHERE
                                                (10000 < A12.CR_RETURN_AMOUNT)
                                                AND (1 < A13.CS_NET_PROFIT)
                                                AND (0 < A13.CS_NET_PAID)
                                                AND (0 < A13.CS_QUANTITY)
                                                AND (2000 = A14.D_YEAR)
                                                AND (12 = A14.D_MOY)
                                            GROUP BY
                                                A13.CS_ITEM_SK
                                        ) A11
                                ) A10
                        ) A9
                ) A8
            WHERE
                (
                    (A8.C2 <= 10)
                    OR (A8.C3 <= 10)
                )
        )
        UNION
        ALL (
            SELECT
                CAST('store' AS VARCHAR(7)) C0,
                A15.C0 C1,
                A15.C1 C2,
                A15.C2 C3,
                A15.C3 C4
            FROM
                (
                    SELECT
                        A16.C0 C0,
                        A16.C1 C1,
                        RANK() OVER(
                            ORDER BY
                                A16.C1 ASC
                        ) C2,
                        A16.C2 C3
                    FROM
                        (
                            SELECT
                                A17.C0 C0,
                                A17.C1 C1,
                                RANK() OVER(
                                    ORDER BY
                                        A17.C2 ASC
                                ) C2
                            FROM
                                (
                                    SELECT
                                        A18.C0 C0,
                                        (
                                            CAST(A18.C1 AS DECIMAL(15, 4)) / CAST(A18.C2 AS DECIMAL(15, 4))
                                        ) C1,
                                        (
                                            CAST(A18.C3 AS DECIMAL(15, 4)) / CAST(A18.C4 AS DECIMAL(15, 4))
                                        ) C2
                                    FROM
                                        (
                                            SELECT
                                                A20.SS_ITEM_SK C0,
                                                SUM(COALESCE(A19.SR_RETURN_QUANTITY, 0)) C1,
                                                SUM(COALESCE(A20.SS_QUANTITY, 0)) C2,
                                                SUM(
                                                    COALESCE(
                                                        CAST(A19.SR_RETURN_AMT AS DECIMAL(13, 2)),
                                                        00000000000.00
                                                    )
                                                ) C3,
                                                SUM(
                                                    COALESCE(
                                                        CAST(A20.SS_NET_PAID AS DECIMAL(13, 2)),
                                                        00000000000.00
                                                    )
                                                ) C4
                                            FROM
                                                (
                                                    STORE_RETURNS A19
                                                    INNER JOIN (
                                                        STORE_SALES A20
                                                        INNER JOIN DATE_DIM A21 ON (A21.D_DATE_SK = A20.SS_SOLD_DATE_SK)
                                                    ) ON (A20.SS_ITEM_SK = A19.SR_ITEM_SK)
                                                    AND (A20.SS_TICKET_NUMBER = A19.SR_TICKET_NUMBER)
                                                )
                                            WHERE
                                                (10000 < A19.SR_RETURN_AMT)
                                                AND (1 < A20.SS_NET_PROFIT)
                                                AND (0 < A20.SS_NET_PAID)
                                                AND (0 < A20.SS_QUANTITY)
                                                AND (2000 = A21.D_YEAR)
                                                AND (12 = A21.D_MOY)
                                            GROUP BY
                                                A20.SS_ITEM_SK
                                        ) A18
                                ) A17
                        ) A16
                ) A15
            WHERE
                (
                    (A15.C2 <= 10)
                    OR (A15.C3 <= 10)
                )
        )
    ) A0
ORDER BY
    1 ASC,
    4 ASC,
    5 ASC,
    2 ASC
limit
    100;