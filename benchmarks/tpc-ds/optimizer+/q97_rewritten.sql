SELECT
    SUM(
        CASE
            WHEN (
                (A0.C0 IS NOT NULL)
                AND (A3.C0 IS NULL)
            ) THEN 1
            ELSE 0
        END
    ) "STORE_ONLY",
    SUM(
        CASE
            WHEN (
                (A0.C0 IS NULL)
                AND (A3.C0 IS NOT NULL)
            ) THEN 1
            ELSE 0
        END
    ) "CATALOG_ONLY",
    SUM(
        CASE
            WHEN (
                (A0.C0 IS NOT NULL)
                AND (A3.C0 IS NOT NULL)
            ) THEN 1
            ELSE 0
        END
    ) "STORE_AND_CATALOG"
FROM
    (
        (
            SELECT
                A1.SS_CUSTOMER_SK C0,
                A1.SS_ITEM_SK C1
            FROM
                (
                    STORE_SALES A1
                    INNER JOIN DATE_DIM A2 ON (A1.SS_SOLD_DATE_SK = A2.D_DATE_SK)
                )
            WHERE
                (1211 <= A2.D_MONTH_SEQ)
                AND (A2.D_MONTH_SEQ <= 1222)
            GROUP BY
                A1.SS_CUSTOMER_SK,
                A1.SS_ITEM_SK
        ) A0
        LEFT OUTER JOIN (
            SELECT
                A4.CS_BILL_CUSTOMER_SK C0,
                A4.CS_ITEM_SK C1
            FROM
                (
                    CATALOG_SALES A4
                    INNER JOIN DATE_DIM A5 ON (A4.CS_SOLD_DATE_SK = A5.D_DATE_SK)
                )
            WHERE
                (1211 <= A5.D_MONTH_SEQ)
                AND (A5.D_MONTH_SEQ <= 1222)
            GROUP BY
                A4.CS_BILL_CUSTOMER_SK,
                A4.CS_ITEM_SK
        ) A3 ON (A0.C0 = A3.C0)
        AND (A0.C1 = A3.C1)
    )
limit
    100;