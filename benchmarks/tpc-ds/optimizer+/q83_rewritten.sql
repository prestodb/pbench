SELECT
    A0.C0 "ITEM_ID",
    A0.C1 "SR_ITEM_QTY",
    (
        ((A0.C1 / ((A0.C1 + A7.C1) + A14.C1)) / 3.0) * 100
    ) "SR_DEV",
    A7.C1 "CR_ITEM_QTY",
    (
        ((A7.C1 / ((A0.C1 + A7.C1) + A14.C1)) / 3.0) * 100
    ) "CR_DEV",
    A14.C1 "WR_ITEM_QTY",
    (
        ((A14.C1 / ((A0.C1 + A7.C1) + A14.C1)) / 3.0) * 100
    ) "WR_DEV",
    (((A0.C1 + A7.C1) + A14.C1) / 3.0) "AVERAGE"
FROM
    (
        (
            (
                SELECT
                    A6.I_ITEM_ID C0,
                    SUM(A1.SR_RETURN_QUANTITY) C1
                FROM
                    (
                        (
                            STORE_RETURNS A1
                            INNER JOIN (
                                DATE_DIM A2
                                INNER JOIN (
                                    SELECT
                                        DISTINCT A4.D_DATE C0
                                    FROM
                                        (
                                            DATE_DIM A4
                                            INNER JOIN DATE_DIM A5 ON (A4.D_WEEK_SEQ = A5.D_WEEK_SEQ)
                                        )
                                    WHERE
                                        (
                                            A5.D_DATE IN ('2000-04-29', '2000-09-09', '2000-11-02')
                                        )
                                ) A3 ON (A2.D_DATE = A3.C0)
                            ) ON (A1.SR_RETURNED_DATE_SK = A2.D_DATE_SK)
                        )
                        INNER JOIN ITEM A6 ON (A1.SR_ITEM_SK = A6.I_ITEM_SK)
                    )
                GROUP BY
                    A6.I_ITEM_ID
            ) A0
            INNER JOIN (
                SELECT
                    A13.I_ITEM_ID C0,
                    SUM(A8.CR_RETURN_QUANTITY) C1
                FROM
                    (
                        (
                            CATALOG_RETURNS A8
                            INNER JOIN (
                                DATE_DIM A9
                                INNER JOIN (
                                    SELECT
                                        DISTINCT A11.D_DATE C0
                                    FROM
                                        (
                                            DATE_DIM A11
                                            INNER JOIN DATE_DIM A12 ON (A11.D_WEEK_SEQ = A12.D_WEEK_SEQ)
                                        )
                                    WHERE
                                        (
                                            A12.D_DATE IN ('2000-04-29', '2000-09-09', '2000-11-02')
                                        )
                                ) A10 ON (A9.D_DATE = A10.C0)
                            ) ON (A8.CR_RETURNED_DATE_SK = A9.D_DATE_SK)
                        )
                        INNER JOIN ITEM A13 ON (A8.CR_ITEM_SK = A13.I_ITEM_SK)
                    )
                GROUP BY
                    A13.I_ITEM_ID
            ) A7 ON (A7.C0 = A0.C0)
        )
        INNER JOIN (
            SELECT
                A20.I_ITEM_ID C0,
                SUM(A15.WR_RETURN_QUANTITY) C1
            FROM
                (
                    (
                        WEB_RETURNS A15
                        INNER JOIN (
                            DATE_DIM A16
                            INNER JOIN (
                                SELECT
                                    DISTINCT A18.D_DATE C0
                                FROM
                                    (
                                        DATE_DIM A18
                                        INNER JOIN DATE_DIM A19 ON (A18.D_WEEK_SEQ = A19.D_WEEK_SEQ)
                                    )
                                WHERE
                                    (
                                        A19.D_DATE IN ('2000-04-29', '2000-09-09', '2000-11-02')
                                    )
                            ) A17 ON (A16.D_DATE = A17.C0)
                        ) ON (A15.WR_RETURNED_DATE_SK = A16.D_DATE_SK)
                    )
                    INNER JOIN ITEM A20 ON (A15.WR_ITEM_SK = A20.I_ITEM_SK)
                )
            GROUP BY
                A20.I_ITEM_ID
        ) A14 ON (A0.C0 = A14.C0)
    )
ORDER BY
    1 ASC
limit
    100;