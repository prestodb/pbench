--#BGBLK 10

--set current schema bdinsights;


SELECT
    "CUSTOMER"."C_CUSTOMER_SK" AS "C",
        "STORE"."S_STORE_SK" AS "C1",
            SUM("STORE_SALES"."SS_NET_PROFIT") AS "C4",
                SUM("STORE_SALES"."SS_LIST_PRICE") AS "C5",
                    SUM("STORE_SALES"."SS_SALES_PRICE") AS "C6",
                        SUM("STORE_SALES"."SS_COUPON_AMT") AS "C7"
                        FROM
                            CUSTOMER
                                    INNER JOIN STORE_SALES
                                            ON "CUSTOMER"."C_CUSTOMER_SK" = "STORE_SALES"."SS_CUSTOMER_SK"

                                                            INNER JOIN  STORE
                                                                            ON "STORE"."S_STORE_SK" = "STORE_SALES"."SS_STORE_SK"
                                                                            WHERE
                                                                                SS_EXT_LIST_PRICE<200
                                                                                    and
                                                                                        "STORE"."S_COUNTRY" IN (
                                                                                                'United States' )
                                                                                        GROUP BY
                                                                                            "CUSTOMER"."C_CUSTOMER_SK",
                                                                                                "STORE"."S_STORE_SK"
                                                                                                order by "CUSTOMER"."C_CUSTOMER_SK",
                                                                                                    "STORE"."S_STORE_SK"
                                                                                                    fetch first 1000 rows only
                                                                                                    ;



--#EOBLK
