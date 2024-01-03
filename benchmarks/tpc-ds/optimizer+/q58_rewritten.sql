SELECT
    A14.C0 "ITEM_ID",
    A14.C1 "SS_ITEM_REV",
    (
        (A14.C1 / (((A14.C1 + A0.C1) + A7.C1) / 3)) * 100
    ) "SS_DEV",
    A0.C1 "CS_ITEM_REV",
    ((A0.C1 / (((A14.C1 + A0.C1) + A7.C1) / 3)) * 100) "CS_DEV",
    A7.C1 "WS_ITEM_REV",
    ((A7.C1 / (((A14.C1 + A0.C1) + A7.C1) / 3)) * 100) "WS_DEV",
    (((A14.C1 + A0.C1) + A7.C1) / 3) "AVERAGE"
FROM
    (
        (
            (
                SELECT
                    A6.I_ITEM_ID C0,
                    SUM(A1.CS_EXT_SALES_PRICE) C1
                FROM
                    (
                        (
                            CATALOG_SALES A1
                            INNER JOIN (
                                DATE_DIM A2
                                INNER JOIN (
                                    SELECT
                                        DISTINCT A4.D_DATE C0
                                    FROM
                                        DATE_DIM A4
                                    WHERE
                                        (
                                            A4.D_WEEK_SEQ = (
                                                SELECT
                                                    A5.D_WEEK_SEQ
                                                FROM
                                                    DATE_DIM A5
                                                WHERE
                                                    (A5.D_DATE = DATE('2000-02-12'))
                                            )
                                        )
                                ) A3 ON (A2.D_DATE = A3.C0)
                            ) ON (A1.CS_SOLD_DATE_SK = A2.D_DATE_SK)
                        )
                        INNER JOIN ITEM A6 ON (A1.CS_ITEM_SK = A6.I_ITEM_SK)
                    )
                GROUP BY
                    A6.I_ITEM_ID
            ) A0
            INNER JOIN (
                SELECT
                    A13.I_ITEM_ID C0,
                    SUM(A8.WS_EXT_SALES_PRICE) C1
                FROM
                    (
                        (
                            WEB_SALES A8
                            INNER JOIN (
                                DATE_DIM A9
                                INNER JOIN (
                                    SELECT
                                        DISTINCT A11.D_DATE C0
                                    FROM
                                        DATE_DIM A11
                                    WHERE
                                        (
                                            A11.D_WEEK_SEQ = (
                                                SELECT
                                                    A12.D_WEEK_SEQ
                                                FROM
                                                    DATE_DIM A12
                                                WHERE
                                                    (A12.D_DATE = DATE('2000-02-12'))
                                            )
                                        )
                                ) A10 ON (A9.D_DATE = A10.C0)
                            ) ON (A8.WS_SOLD_DATE_SK = A9.D_DATE_SK)
                        )
                        INNER JOIN ITEM A13 ON (A8.WS_ITEM_SK = A13.I_ITEM_SK)
                    )
                GROUP BY
                    A13.I_ITEM_ID
            ) A7 ON (A7.C0 = A0.C0)
            AND ((0.9 * A7.C1) <= A0.C1)
            AND (A0.C1 <= (1.1 * A7.C1))
            AND ((0.9 * A0.C1) <= A7.C1)
            AND (A7.C1 <= (1.1 * A0.C1))
        )
        INNER JOIN (
            SELECT
                A20.I_ITEM_ID C0,
                SUM(A15.SS_EXT_SALES_PRICE) C1
            FROM
                (
                    (
                        STORE_SALES A15
                        INNER JOIN (
                            DATE_DIM A16
                            INNER JOIN (
                                SELECT
                                    DISTINCT A18.D_DATE C0
                                FROM
                                    DATE_DIM A18
                                WHERE
                                    (
                                        A18.D_WEEK_SEQ = (
                                            SELECT
                                                A19.D_WEEK_SEQ
                                            FROM
                                                DATE_DIM A19
                                            WHERE
                                                (A19.D_DATE = DATE('2000-02-12'))
                                        )
                                    )
                            ) A17 ON (A16.D_DATE = A17.C0)
                        ) ON (A15.SS_SOLD_DATE_SK = A16.D_DATE_SK)
                    )
                    INNER JOIN ITEM A20 ON (A15.SS_ITEM_SK = A20.I_ITEM_SK)
                )
            GROUP BY
                A20.I_ITEM_ID
        ) A14 ON (A0.C0 = A14.C0)
        AND ((0.9 * A0.C1) <= A14.C1)
        AND (A14.C1 <= (1.1 * A0.C1))
        AND ((0.9 * A7.C1) <= A14.C1)
        AND (A14.C1 <= (1.1 * A7.C1))
        AND ((0.9 * A14.C1) <= A0.C1)
        AND (A0.C1 <= (1.1 * A14.C1))
        AND ((0.9 * A14.C1) <= A7.C1)
        AND (A7.C1 <= (1.1 * A14.C1))
    )
ORDER BY
    1 ASC
limit
    100;