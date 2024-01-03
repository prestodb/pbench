WITH A0 AS (
    SELECT
        A8.I_PRODUCT_NAME C0,
        A8.I_ITEM_SK C1,
        A5.S_STORE_NAME C2,
        A5.S_ZIP C3,
        A1.CA_STREET_NUMBER C4,
        A1.CA_STREET_NAME C5,
        A1.CA_CITY C6,
        A1.CA_ZIP C7,
        A14.CA_STREET_NUMBER C8,
        A14.CA_STREET_NAME C9,
        A14.CA_CITY C10,
        A14.CA_ZIP C11,
        A3.D_YEAR C12,
        COUNT(*) C13,
        SUM(A2.SS_WHOLESALE_COST) C14,
        SUM(A2.SS_LIST_PRICE) C15,
        SUM(A2.SS_COUPON_AMT) C16
    FROM
        (
            (
                (
                    (
                        CUSTOMER_ADDRESS A1
                        INNER JOIN (
                            (
                                (
                                    (
                                        STORE_SALES A2
                                        INNER JOIN DATE_DIM A3 ON (A2.SS_SOLD_DATE_SK = A3.D_DATE_SK)
                                    )
                                    INNER JOIN HOUSEHOLD_DEMOGRAPHICS A4 ON (A2.SS_HDEMO_SK = A4.HD_DEMO_SK)
                                )
                                INNER JOIN STORE A5 ON (A2.SS_STORE_SK = A5.S_STORE_SK)
                            )
                            INNER JOIN CUSTOMER_DEMOGRAPHICS A6 ON (A2.SS_CDEMO_SK = A6.CD_DEMO_SK)
                        ) ON (A2.SS_ADDR_SK = A1.CA_ADDRESS_SK)
                    )
                    INNER JOIN (
                        STORE_RETURNS A7
                        INNER JOIN ITEM A8 ON (A8.I_ITEM_SK = A7.SR_ITEM_SK)
                    ) ON (A2.SS_TICKET_NUMBER = A7.SR_TICKET_NUMBER)
                    AND (A7.SR_ITEM_SK = A2.SS_ITEM_SK)
                )
                INNER JOIN (
                    (
                        (
                            (
                                (
                                    CUSTOMER A9
                                    INNER JOIN HOUSEHOLD_DEMOGRAPHICS A10 ON (A9.C_CURRENT_HDEMO_SK = A10.HD_DEMO_SK)
                                )
                                INNER JOIN DATE_DIM A11 ON (A9.C_FIRST_SALES_DATE_SK = A11.D_DATE_SK)
                            )
                            INNER JOIN DATE_DIM A12 ON (A9.C_FIRST_SHIPTO_DATE_SK = A12.D_DATE_SK)
                        )
                        INNER JOIN CUSTOMER_DEMOGRAPHICS A13 ON (
                            CAST(A9.C_CURRENT_CDEMO_SK AS BIGINT) = A13.CD_DEMO_SK
                        )
                    )
                    INNER JOIN CUSTOMER_ADDRESS A14 ON (
                        CAST(A9.C_CURRENT_ADDR_SK AS BIGINT) = A14.CA_ADDRESS_SK
                    )
                ) ON (A2.SS_CUSTOMER_SK = A9.C_CUSTOMER_SK)
                AND (A6.CD_MARITAL_STATUS <> A13.CD_MARITAL_STATUS)
            )
            INNER JOIN (
                SELECT
                    SUM(A17.CS_EXT_LIST_PRICE) C0,
                    SUM(
                        (
                            (A16.CR_REFUNDED_CASH + A16.CR_REVERSED_CHARGE) + A16.CR_STORE_CREDIT
                        )
                    ) C1,
                    A17.CS_ITEM_SK C2
                FROM
                    (
                        CATALOG_RETURNS A16
                        INNER JOIN CATALOG_SALES A17 ON (A17.CS_ITEM_SK = A16.CR_ITEM_SK)
                        AND (A17.CS_ORDER_NUMBER = A16.CR_ORDER_NUMBER)
                    )
                GROUP BY
                    A17.CS_ITEM_SK
            ) A15 ON (A2.SS_ITEM_SK = A15.C2)
        )
    WHERE
        (A2.SS_PROMO_SK IS NOT NULL)
        AND (A3.D_YEAR IN (2002, 2001))
        AND (A4.HD_INCOME_BAND_SK IS NOT NULL)
        AND (
            A8.I_COLOR IN (
                'light',
                'cyan',
                'burnished',
                'green',
                'almond',
                'smoke'
            )
        )
        AND (22 <= A8.I_CURRENT_PRICE)
        AND (A8.I_CURRENT_PRICE <= 32)
        AND (23 <= A8.I_CURRENT_PRICE)
        AND (A8.I_CURRENT_PRICE <= 37)
        AND (A10.HD_INCOME_BAND_SK IS NOT NULL)
        AND ((2 * A15.C1) < A15.C0)
    GROUP BY
        A8.I_ITEM_SK,
        A5.S_STORE_NAME,
        A5.S_ZIP,
        A1.CA_STREET_NUMBER,
        A1.CA_STREET_NAME,
        A1.CA_CITY,
        A1.CA_ZIP,
        A14.CA_STREET_NUMBER,
        A14.CA_STREET_NAME,
        A14.CA_CITY,
        A14.CA_ZIP,
        A3.D_YEAR,
        A11.D_YEAR,
        A12.D_YEAR,
        A8.I_PRODUCT_NAME
)
SELECT
    "A18".C0 "PRODUCT_NAME",
    "A18".C2 "STORE_NAME",
    "A18".C3 "STORE_ZIP",
    "A18".C4 "B_STREET_NUMBER",
    "A18".C5 "B_STREET_NAME",
    "A18".C6 "B_CITY",
    "A18".C7 "B_ZIP",
    "A18".C8 "C_STREET_NUMBER",
    "A18".C9 "C_STREET_NAME",
    "A18".C10 "C_CITY",
    "A18".C11 "C_ZIP",
    "A18".C12 "SYEAR1",
    "A18".C13 "CNT1",
    "A18".C14 "S11",
    "A18".C15 "S21",
    "A18".C16 "S31",
    "A19".C14 "S12",
    "A19".C15 "S22",
    "A19".C16 "S32",
    "A19".C12 "SYEAR2",
    "A19".C13 "CNT2"
FROM
    (
        A0 "A18"
        INNER JOIN A0 "A19" ON ("A18".C1 = "A19".C1)
        AND ("A18".C2 = "A19".C2)
        AND ("A18".C3 = "A19".C3)
        AND ("A19".C13 <= "A18".C13)
    )
WHERE
    ("A18".C12 = 2001)
    AND ("A19".C12 = 2002)
ORDER BY
    1 ASC,
    2 ASC,
    21 ASC,
    14 ASC,
    17 ASC
limit
    100;