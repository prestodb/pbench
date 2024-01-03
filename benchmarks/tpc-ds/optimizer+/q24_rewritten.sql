WITH A1 AS (
    SELECT
        A6.C_LAST_NAME C0,
        A6.C_FIRST_NAME C1,
        A4.S_STORE_NAME C2,
        A7.I_COLOR C3,
        SUM(A3.SS_NET_PROFIT) C4
    FROM
        (
            STORE_RETURNS A2
            INNER JOIN (
                (
                    (
                        STORE_SALES A3
                        INNER JOIN STORE A4 ON (A3.SS_STORE_SK = A4.S_STORE_SK)
                    )
                    INNER JOIN (
                        CUSTOMER_ADDRESS A5
                        INNER JOIN CUSTOMER A6 ON (
                            CAST(A6.C_CURRENT_ADDR_SK AS BIGINT) = A5.CA_ADDRESS_SK
                        )
                        AND (A6.C_BIRTH_COUNTRY <> UPPER(A5.CA_COUNTRY))
                    ) ON (A3.SS_CUSTOMER_SK = A6.C_CUSTOMER_SK)
                    AND (A4.S_ZIP = A5.CA_ZIP)
                )
                INNER JOIN ITEM A7 ON (A7.I_ITEM_SK = A3.SS_ITEM_SK)
            ) ON (A3.SS_TICKET_NUMBER = A2.SR_TICKET_NUMBER)
            AND (A3.SS_ITEM_SK = A2.SR_ITEM_SK)
        )
    WHERE
        (A4.S_MARKET_ID = 10)
    GROUP BY
        A6.C_LAST_NAME,
        A6.C_FIRST_NAME,
        A4.S_STORE_NAME,
        A7.I_COLOR,
        A5.CA_STATE,
        A4.S_STATE,
        A7.I_CURRENT_PRICE,
        A7.I_MANAGER_ID,
        A7.I_UNITS,
        A7.I_SIZE
)
SELECT
    A0.C1 "C_LAST_NAME",
    A0.C2 "C_FIRST_NAME",
    A0.C3 "S_STORE_NAME",
    A0.C0 "PAID"
FROM
    (
        SELECT
            SUM("A8".C4) C0,
            "A8".C0 C1,
            "A8".C1 C2,
            "A8".C2 C3
        FROM
            A1 "A8"
        WHERE
            ("A8".C3 = 'orchid              ')
        GROUP BY
            "A8".C0,
            "A8".C1,
            "A8".C2
    ) A0
WHERE
    (
        (
            0.05 * (
                (
                    SELECT
                        SUM("A9".C4)
                    FROM
                        A1 "A9"
                ) / (
                    SELECT
                        COUNT("A10".C4)
                    FROM
                        A1 "A10"
                )
            )
        ) < A0.C0
    )
ORDER BY
    1 ASC,
    2 ASC,
    3 ASC
limit
    100;