WITH A1 AS (
    SELECT
        A6.CA_STATE C0,
        A7.I_CATEGORY C1,
        A7.I_CURRENT_PRICE C2
    FROM
        (
            (
                (
                    CUSTOMER A2
                    INNER JOIN (
                        STORE_SALES A3
                        INNER JOIN DATE_DIM A4 ON (A3.SS_SOLD_DATE_SK = A4.D_DATE_SK)
                    ) ON (A2.C_CUSTOMER_SK = A3.SS_CUSTOMER_SK)
                )
                INNER JOIN CUSTOMER_ADDRESS A6 ON (
                    A6.CA_ADDRESS_SK = CAST(A2.C_CURRENT_ADDR_SK AS BIGINT)
                )
            )
            INNER JOIN ITEM A7 ON (A3.SS_ITEM_SK = A7.I_ITEM_SK)
        )
    WHERE
        (
            A4.D_MONTH_SEQ = (
                SELECT
                    DISTINCT A5.D_MONTH_SEQ
                FROM
                    DATE_DIM A5
                WHERE
                    (A5.D_YEAR = 1998)
                    AND (A5.D_MOY = 3)
            )
        )
)
SELECT
    A0.C1 "STATE",
    A0.C0 "CNT"
FROM
    (
        SELECT
            COUNT(*) C0,
            "A8".C0 C1
        FROM
            (
                A1 "A8"
                INNER JOIN (
                    SELECT
                        SUM(A11.C1) C0,
                        SUM(A11.C0) C1,
                        A12.C0 C2
                    FROM
                        (
                            (
                                SELECT
                                    COUNT(A10.I_CURRENT_PRICE) C0,
                                    SUM(A10.I_CURRENT_PRICE) C1,
                                    A10.I_CATEGORY C2
                                FROM
                                    ITEM A10
                                GROUP BY
                                    A10.I_CATEGORY A11
                            )
                            INNER JOIN (
                                SELECT
                                    DISTINCT "A13".C1 C0
                                FROM
                                    A1 "A13"
                            ) A12 ON (A11.C2 = A12.C0)
                        )
                    GROUP BY
                        A12.C0
                ) A9 ON (A9.C2 = "A8".C1)
                AND ((1.2 * (A9.C0 / A9.C1)) < "A8".C2)
            )
        GROUP BY
            "A8".C0
    ) A0
WHERE
    (10 <= A0.C0)
ORDER BY
    2 ASC,
    1 ASC
limit
    100;