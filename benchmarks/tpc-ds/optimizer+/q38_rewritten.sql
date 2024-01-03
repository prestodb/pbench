SELECT
    COUNT(*)
FROM
    (
        SELECT
            A1.C0 C0,
            A1.C1 C1,
            A1.C2 C2,
            SUM(A1.C3) C3,
            COUNT(*) C4
        FROM
            (
                (
                    SELECT
                        A2.C_LAST_NAME C0,
                        A2.C_FIRST_NAME C1,
                        A4.D_DATE C2,
                        -1 C3
                    FROM
                        (
                            CUSTOMER A2
                            INNER JOIN (
                                WEB_SALES A3
                                INNER JOIN DATE_DIM A4 ON (A3.WS_SOLD_DATE_SK = A4.D_DATE_SK)
                            ) ON (A3.WS_BILL_CUSTOMER_SK = A2.C_CUSTOMER_SK)
                        )
                    WHERE
                        (1190 <= A4.D_MONTH_SEQ)
                        AND (A4.D_MONTH_SEQ <= 1201)
                )
                UNION
                ALL (
                    SELECT
                        A5.C0 C0,
                        A5.C1 C1,
                        A5.C2 C2,
                        1 C3
                    FROM
                        (
                            SELECT
                                A6.C0 C0,
                                A6.C1 C1,
                                A6.C2 C2,
                                SUM(A6.C3) C3,
                                COUNT(*) C4
                            FROM
                                (
                                    (
                                        SELECT
                                            A7.C_LAST_NAME C0,
                                            A7.C_FIRST_NAME C1,
                                            A9.D_DATE C2,
                                            -1 C3
                                        FROM
                                            (
                                                CUSTOMER A7
                                                INNER JOIN (
                                                    CATALOG_SALES A8
                                                    INNER JOIN DATE_DIM A9 ON (A8.CS_SOLD_DATE_SK = A9.D_DATE_SK)
                                                ) ON (A8.CS_BILL_CUSTOMER_SK = A7.C_CUSTOMER_SK)
                                            )
                                        WHERE
                                            (1190 <= A9.D_MONTH_SEQ)
                                            AND (A9.D_MONTH_SEQ <= 1201)
                                    )
                                    UNION
                                    ALL (
                                        SELECT
                                            A10.C_LAST_NAME C0,
                                            A10.C_FIRST_NAME C1,
                                            A12.D_DATE C2,
                                            1 C3
                                        FROM
                                            (
                                                CUSTOMER A10
                                                INNER JOIN (
                                                    STORE_SALES A11
                                                    INNER JOIN DATE_DIM A12 ON (A11.SS_SOLD_DATE_SK = A12.D_DATE_SK)
                                                ) ON (A11.SS_CUSTOMER_SK = A10.C_CUSTOMER_SK)
                                            )
                                        WHERE
                                            (1190 <= A12.D_MONTH_SEQ)
                                            AND (A12.D_MONTH_SEQ <= 1201)
                                    )
                                ) A6
                            GROUP BY
                                A6.C2,
                                A6.C1,
                                A6.C0
                        ) A5
                    WHERE
                        (
                            (
                                A5.C4 - CASE
                                    WHEN (A5.C3 >= 0) THEN A5.C3
                                    ELSE (-(A5.C3))
                                END
                            ) >= 2
                        )
                )
            ) A1
        GROUP BY
            A1.C2,
            A1.C1,
            A1.C0
    ) A0
WHERE
    (
        (
            A0.C4 - CASE
                WHEN (A0.C3 >= 0) THEN A0.C3
                ELSE (-(A0.C3))
            END
        ) >= 2
    )
limit
    100;