SELECT
    (A10.C1 / A10.C2) "B1_LP",
    CAST(A10.C2 AS INTEGER) "B1_CNT",
    A10.C0 "B1_CNTD",
    (A0.C1 / A0.C2) "B2_LP",
    CAST(A0.C2 AS INTEGER) "B2_CNT",
    A0.C0 "B2_CNTD",
    (A8.C1 / A8.C2) "B3_LP",
    CAST(A8.C2 AS INTEGER) "B3_CNT",
    A8.C0 "B3_CNTD",
    (A2.C1 / A2.C2) "B4_LP",
    CAST(A2.C2 AS INTEGER) "B4_CNT",
    A2.C0 "B4_CNTD",
    (A6.C1 / A6.C2) "B5_LP",
    CAST(A6.C2 AS INTEGER) "B5_CNT",
    A6.C0 "B5_CNTD",
    (A4.C1 / A4.C2) "B6_LP",
    CAST(A4.C2 AS INTEGER) "B6_CNT",
    A4.C0 "B6_CNTD"
FROM
    (
        SELECT
            COUNT(DISTINCT A1.SS_LIST_PRICE) C0,
            SUM(A1.SS_LIST_PRICE) C1,
            COUNT(A1.SS_LIST_PRICE) C2
        FROM
            STORE_SALES A1
        WHERE
            (
                (
                    (
                        (A1.SS_LIST_PRICE >= 143)
                        AND (A1.SS_LIST_PRICE <= 153)
                    )
                    OR (
                        (A1.SS_COUPON_AMT >= 5562)
                        AND (A1.SS_COUPON_AMT <= 6562)
                    )
                )
                OR (
                    (A1.SS_WHOLESALE_COST >= 45)
                    AND (A1.SS_WHOLESALE_COST <= 65)
                )
            )
            AND (6 <= A1.SS_QUANTITY)
            AND (A1.SS_QUANTITY <= 10)
    ) A0,
    (
        SELECT
            COUNT(DISTINCT A3.SS_LIST_PRICE) C0,
            SUM(A3.SS_LIST_PRICE) C1,
            COUNT(A3.SS_LIST_PRICE) C2
        FROM
            STORE_SALES A3
        WHERE
            (
                (
                    (
                        (A3.SS_LIST_PRICE >= 24)
                        AND (A3.SS_LIST_PRICE <= 34)
                    )
                    OR (
                        (A3.SS_COUPON_AMT >= 3706)
                        AND (A3.SS_COUPON_AMT <= 4706)
                    )
                )
                OR (
                    (A3.SS_WHOLESALE_COST >= 46)
                    AND (A3.SS_WHOLESALE_COST <= 66)
                )
            )
            AND (16 <= A3.SS_QUANTITY)
            AND (A3.SS_QUANTITY <= 20)
    ) A2,
    (
        SELECT
            COUNT(DISTINCT A5.SS_LIST_PRICE) C0,
            SUM(A5.SS_LIST_PRICE) C1,
            COUNT(A5.SS_LIST_PRICE) C2
        FROM
            STORE_SALES A5
        WHERE
            (
                (
                    (
                        (A5.SS_LIST_PRICE >= 169)
                        AND (A5.SS_LIST_PRICE <= 179)
                    )
                    OR (
                        (A5.SS_COUPON_AMT >= 10672)
                        AND (A5.SS_COUPON_AMT <= 11672)
                    )
                )
                OR (
                    (A5.SS_WHOLESALE_COST >= 58)
                    AND (A5.SS_WHOLESALE_COST <= 78)
                )
            )
            AND (26 <= A5.SS_QUANTITY)
            AND (A5.SS_QUANTITY <= 30)
    ) A4,
    (
        SELECT
            COUNT(DISTINCT A7.SS_LIST_PRICE) C0,
            SUM(A7.SS_LIST_PRICE) C1,
            COUNT(A7.SS_LIST_PRICE) C2
        FROM
            STORE_SALES A7
        WHERE
            (
                (
                    (
                        (A7.SS_LIST_PRICE >= 76)
                        AND (A7.SS_LIST_PRICE <= 86)
                    )
                    OR (
                        (A7.SS_COUPON_AMT >= 2096)
                        AND (A7.SS_COUPON_AMT <= 3096)
                    )
                )
                OR (
                    (A7.SS_WHOLESALE_COST >= 50)
                    AND (A7.SS_WHOLESALE_COST <= 70)
                )
            )
            AND (21 <= A7.SS_QUANTITY)
            AND (A7.SS_QUANTITY <= 25)
    ) A6,
    (
        SELECT
            COUNT(DISTINCT A9.SS_LIST_PRICE) C0,
            SUM(A9.SS_LIST_PRICE) C1,
            COUNT(A9.SS_LIST_PRICE) C2
        FROM
            STORE_SALES A9
        WHERE
            (
                (
                    (
                        (A9.SS_LIST_PRICE >= 159)
                        AND (A9.SS_LIST_PRICE <= 169)
                    )
                    OR (
                        (A9.SS_COUPON_AMT >= 2807)
                        AND (A9.SS_COUPON_AMT <= 3807)
                    )
                )
                OR (
                    (A9.SS_WHOLESALE_COST >= 24)
                    AND (A9.SS_WHOLESALE_COST <= 44)
                )
            )
            AND (11 <= A9.SS_QUANTITY)
            AND (A9.SS_QUANTITY <= 15)
    ) A8,
    (
        SELECT
            COUNT(DISTINCT A11.SS_LIST_PRICE) C0,
            SUM(A11.SS_LIST_PRICE) C1,
            COUNT(A11.SS_LIST_PRICE) C2
        FROM
            STORE_SALES A11
        WHERE
            (
                (
                    (
                        (A11.SS_LIST_PRICE >= 28)
                        AND (A11.SS_LIST_PRICE <= 38)
                    )
                    OR (
                        (A11.SS_COUPON_AMT >= 12573)
                        AND (A11.SS_COUPON_AMT <= 13573)
                    )
                )
                OR (
                    (A11.SS_WHOLESALE_COST >= 33)
                    AND (A11.SS_WHOLESALE_COST <= 53)
                )
            )
            AND (0 <= A11.SS_QUANTITY)
            AND (A11.SS_QUANTITY <= 5)
    ) A10
limit
    100;