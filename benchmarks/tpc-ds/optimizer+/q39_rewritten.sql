WITH A0 AS (
    SELECT
        A1.INV_WAREHOUSE_SK C0,
        A1.INV_ITEM_SK C1,
        A2.D_MOY C2,
        STDDEV_SAMP(A1.INV_QUANTITY_ON_HAND) C3,
        SUM(A1.INV_QUANTITY_ON_HAND) C4,
        COUNT(A1.INV_QUANTITY_ON_HAND) C5
    FROM
        (
            INVENTORY A1
            INNER JOIN DATE_DIM A2 ON (A1.INV_DATE_SK = A2.D_DATE_SK)
        )
    WHERE
        (A2.D_MOY IN (2, 1))
        AND (A2.D_YEAR = 2001)
    GROUP BY
        A1.INV_WAREHOUSE_SK,
        A1.INV_ITEM_SK,
        A2.D_MOY
)
SELECT
    "A4".C0 "inv1 w_warehouse_sk",
    "A4".C1 "inv1.i_item_sk",
    "A4".C2 "inv1.d_moy",
    CAST(("A4".C4 / "A4".C5) AS INTEGER) "inv1.mean",
    CASE
        WHEN (CAST(("A4".C4 / "A4".C5) AS INTEGER) = 0) THEN NULL
        ELSE ("A4".C3 / CAST(("A4".C4 / "A4".C5) AS INTEGER))
    END "inv1.cov",
    "A3".C0 "inv2.w_warehouse_sk",
    "A3".C1 "inv2.i_item_sk",
    "A3".C2 "inv2.d_moy",
    CAST(("A3".C4 / "A3".C5) AS INTEGER) "inv2.mean",
    CASE
        WHEN (CAST(("A3".C4 / "A3".C5) AS INTEGER) = 0) THEN NULL
        ELSE ("A3".C3 / CAST(("A3".C4 / "A3".C5) AS INTEGER))
    END "inv2.cov"
FROM
    (
        A0 "A3"
        INNER JOIN A0 "A4" ON ("A4".C1 = "A3".C1)
        AND ("A4".C0 = "A3".C0)
    )
WHERE
    (
        + 1.0000000000000000E + 000 < CASE
            WHEN (CAST(("A3".C4 / "A3".C5) AS INTEGER) = 0) THEN + 0.0000000000000000E + 000
            ELSE ("A3".C3 / CAST(("A3".C4 / "A3".C5) AS INTEGER))
        END
    )
    AND ("A3".C2 = 2)
    AND (
        + 1.0000000000000000E + 000 < CASE
            WHEN (CAST(("A4".C4 / "A4".C5) AS INTEGER) = 0) THEN + 0.0000000000000000E + 000
            ELSE ("A4".C3 / CAST(("A4".C4 / "A4".C5) AS INTEGER))
        END
    )
    AND ("A4".C2 = 1)
ORDER BY
    1 ASC,
    2 ASC;