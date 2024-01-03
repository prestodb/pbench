SELECT
    A0.C0 "W_WAREHOUSE_NAME",
    A0.C1 "W_WAREHOUSE_SQ_FT",
    A0.C2 "W_CITY",
    A0.C3 "W_COUNTY",
    A0.C4 "W_STATE",
    A0.C5 "W_COUNTRY",
    A0.C6 "SHIP_CARRIERS",
    A0.C7 "YEAR",
    SUM(A0.C8) "JAN_SALES",
    SUM(A0.C9) "FEB_SALES",
    SUM(A0.C10) "MAR_SALES",
    SUM(A0.C11) "APR_SALES",
    SUM(A0.C12) "MAY_SALES",
    SUM(A0.C13) "JUN_SALES",
    SUM(A0.C14) "JUL_SALES",
    SUM(A0.C15) "AUG_SALES",
    SUM(A0.C16) "SEP_SALES",
    SUM(A0.C17) "OCT_SALES",
    SUM(A0.C18) "NOV_SALES",
    SUM(A0.C19) "DEC_SALES",
    SUM((A0.C8 / A0.C1)) "JAN_SALES_PER_SQ_FOOT",
    SUM((A0.C9 / A0.C1)) "FEB_SALES_PER_SQ_FOOT",
    SUM((A0.C10 / A0.C1)) "MAR_SALES_PER_SQ_FOOT",
    SUM((A0.C11 / A0.C1)) "APR_SALES_PER_SQ_FOOT",
    SUM((A0.C12 / A0.C1)) "MAY_SALES_PER_SQ_FOOT",
    SUM((A0.C13 / A0.C1)) "JUN_SALES_PER_SQ_FOOT",
    SUM((A0.C14 / A0.C1)) "JUL_SALES_PER_SQ_FOOT",
    SUM((A0.C15 / A0.C1)) "AUG_SALES_PER_SQ_FOOT",
    SUM((A0.C16 / A0.C1)) "SEP_SALES_PER_SQ_FOOT",
    SUM((A0.C17 / A0.C1)) "OCT_SALES_PER_SQ_FOOT",
    SUM((A0.C18 / A0.C1)) "NOV_SALES_PER_SQ_FOOT",
    SUM((A0.C19 / A0.C1)) "DEC_SALES_PER_SQ_FOOT",
    SUM(A0.C20) "JAN_NET",
    SUM(A0.C21) "FEB_NET",
    SUM(A0.C22) "MAR_NET",
    SUM(A0.C23) "APR_NET",
    SUM(A0.C24) "MAY_NET",
    SUM(A0.C25) "JUN_NET",
    SUM(A0.C26) "JUL_NET",
    SUM(A0.C27) "AUG_NET",
    SUM(A0.C28) "SEP_NET",
    SUM(A0.C29) "OCT_NET",
    SUM(A0.C30) "NOV_NET",
    SUM(A0.C31) "DEC_NET"
FROM
    (
        (
            SELECT
                A1.C0 C0,
                A1.C1 C1,
                A1.C2 C2,
                A1.C3 C3,
                A1.C4 C4,
                A1.C5 C5,
                'FEDEX,GERMA' C6,
                A1.C6 C7,
                A1.C7 C8,
                A1.C8 C9,
                A1.C9 C10,
                A1.C10 C11,
                A1.C11 C12,
                A1.C12 C13,
                A1.C13 C14,
                A1.C14 C15,
                A1.C15 C16,
                A1.C16 C17,
                A1.C17 C18,
                A1.C18 C19,
                A1.C19 C20,
                A1.C20 C21,
                A1.C21 C22,
                A1.C22 C23,
                A1.C23 C24,
                A1.C24 C25,
                A1.C25 C26,
                A1.C26 C27,
                A1.C27 C28,
                A1.C28 C29,
                A1.C29 C30,
                A1.C30 C31
            FROM
                (
                    SELECT
                        A6.W_WAREHOUSE_NAME C0,
                        A6.W_WAREHOUSE_SQ_FT C1,
                        A6.W_CITY C2,
                        A6.W_COUNTY C3,
                        A6.W_STATE C4,
                        A6.W_COUNTRY C5,
                        A3.D_YEAR C6,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 1) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C7,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 2) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C8,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 3) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C9,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 4) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C10,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 5) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C11,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 6) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C12,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 7) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C13,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 8) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C14,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 9) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C15,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 10) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C16,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 11) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C17,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 12) THEN (A2.CS_SALES_PRICE * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C18,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 1) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C19,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 2) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C20,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 3) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C21,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 4) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C22,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 5) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C23,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 6) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C24,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 7) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C25,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 8) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C26,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 9) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C27,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 10) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C28,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 11) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C29,
                        SUM(
                            CASE
                                WHEN (A3.D_MOY = 12) THEN (A2.CS_NET_PAID * A2.CS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C30
                    FROM
                        (
                            (
                                (
                                    (
                                        CATALOG_SALES A2
                                        INNER JOIN DATE_DIM A3 ON (A2.CS_SOLD_DATE_SK = A3.D_DATE_SK)
                                    )
                                    INNER JOIN TIME_DIM A4 ON (A2.CS_SOLD_TIME_SK = A4.T_TIME_SK)
                                )
                                INNER JOIN SHIP_MODE A5 ON (A2.CS_SHIP_MODE_SK = A5.SM_SHIP_MODE_SK)
                            )
                            INNER JOIN WAREHOUSE A6 ON (A2.CS_WAREHOUSE_SK = A6.W_WAREHOUSE_SK)
                        )
                    WHERE
                        (A3.D_YEAR = 2001)
                        AND (19072 <= A4.T_TIME)
                        AND (A4.T_TIME <= 47872)
                        AND (A5.SM_CARRIER IN ('FEDEX', 'GERMA'))
                    GROUP BY
                        A6.W_WAREHOUSE_NAME,
                        A6.W_WAREHOUSE_SQ_FT,
                        A6.W_CITY,
                        A6.W_COUNTY,
                        A6.W_STATE,
                        A6.W_COUNTRY,
                        A3.D_YEAR
                ) A1
        )
        UNION
        ALL (
            SELECT
                A7.C0 C0,
                A7.C1 C1,
                A7.C2 C2,
                A7.C3 C3,
                A7.C4 C4,
                A7.C5 C5,
                'FEDEX,GERMA' C6,
                A7.C6 C7,
                A7.C7 C8,
                A7.C8 C9,
                A7.C9 C10,
                A7.C10 C11,
                A7.C11 C12,
                A7.C12 C13,
                A7.C13 C14,
                A7.C14 C15,
                A7.C15 C16,
                A7.C16 C17,
                A7.C17 C18,
                A7.C18 C19,
                A7.C19 C20,
                A7.C20 C21,
                A7.C21 C22,
                A7.C22 C23,
                A7.C23 C24,
                A7.C24 C25,
                A7.C25 C26,
                A7.C26 C27,
                A7.C27 C28,
                A7.C28 C29,
                A7.C29 C30,
                A7.C30 C31
            FROM
                (
                    SELECT
                        A12.W_WAREHOUSE_NAME C0,
                        A12.W_WAREHOUSE_SQ_FT C1,
                        A12.W_CITY C2,
                        A12.W_COUNTY C3,
                        A12.W_STATE C4,
                        A12.W_COUNTRY C5,
                        A9.D_YEAR C6,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 1) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C7,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 2) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C8,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 3) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C9,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 4) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C10,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 5) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C11,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 6) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C12,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 7) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C13,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 8) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C14,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 9) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C15,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 10) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C16,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 11) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C17,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 12) THEN (A8.WS_EXT_LIST_PRICE * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C18,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 1) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C19,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 2) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C20,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 3) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C21,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 4) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C22,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 5) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C23,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 6) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C24,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 7) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C25,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 8) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C26,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 9) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C27,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 10) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C28,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 11) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C29,
                        SUM(
                            CASE
                                WHEN (A9.D_MOY = 12) THEN (A8.WS_NET_PROFIT * A8.WS_QUANTITY)
                                ELSE 0000000000000000.00
                            END
                        ) C30
                    FROM
                        (
                            (
                                (
                                    (
                                        WEB_SALES A8
                                        INNER JOIN DATE_DIM A9 ON (A8.WS_SOLD_DATE_SK = A9.D_DATE_SK)
                                    )
                                    INNER JOIN TIME_DIM A10 ON (A8.WS_SOLD_TIME_SK = A10.T_TIME_SK)
                                )
                                INNER JOIN SHIP_MODE A11 ON (A8.WS_SHIP_MODE_SK = A11.SM_SHIP_MODE_SK)
                            )
                            INNER JOIN WAREHOUSE A12 ON (A8.WS_WAREHOUSE_SK = A12.W_WAREHOUSE_SK)
                        )
                    WHERE
                        (A9.D_YEAR = 2001)
                        AND (19072 <= A10.T_TIME)
                        AND (A10.T_TIME <= 47872)
                        AND (A11.SM_CARRIER IN ('FEDEX', 'GERMA'))
                    GROUP BY
                        A12.W_WAREHOUSE_NAME,
                        A12.W_WAREHOUSE_SQ_FT,
                        A12.W_CITY,
                        A12.W_COUNTY,
                        A12.W_STATE,
                        A12.W_COUNTRY,
                        A9.D_YEAR
                ) A7
        )
    ) A0
GROUP BY
    A0.C0,
    A0.C1,
    A0.C2,
    A0.C3,
    A0.C4,
    A0.C5,
    A0.C6,
    A0.C7
ORDER BY
    1 ASC
limit
    100;