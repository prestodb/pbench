SELECT
    A35.C0 "H8_30_TO_9",
    A30.C0 "H9_TO_9_30",
    A25.C0 "H9_30_TO_10",
    A20.C0 "H10_TO_10_30",
    A15.C0 "H10_30_TO_11",
    A10.C0 "H11_TO_11_30",
    A0.C0 "H11_30_TO_12",
    A5.C0 "H12_TO_12_30"
FROM
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A1
                        INNER JOIN TIME_DIM A2 ON (A1.SS_SOLD_TIME_SK = A2.T_TIME_SK)
                    )
                    INNER JOIN STORE A3 ON (A1.SS_STORE_SK = A3.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A4 ON (A1.SS_HDEMO_SK = A4.HD_DEMO_SK)
            )
        WHERE
            (A2.T_HOUR = 11)
            AND (30 <= A2.T_MINUTE)
            AND (A3.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A4.HD_DEP_COUNT = 2)
                        AND (A4.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A4.HD_DEP_COUNT = 1)
                        AND (A4.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A4.HD_DEP_COUNT = 4)
                    AND (A4.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A0,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A6
                        INNER JOIN TIME_DIM A7 ON (A6.SS_SOLD_TIME_SK = A7.T_TIME_SK)
                    )
                    INNER JOIN STORE A8 ON (A6.SS_STORE_SK = A8.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A9 ON (A6.SS_HDEMO_SK = A9.HD_DEMO_SK)
            )
        WHERE
            (A7.T_HOUR = 12)
            AND (A7.T_MINUTE < 30)
            AND (A8.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A9.HD_DEP_COUNT = 2)
                        AND (A9.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A9.HD_DEP_COUNT = 1)
                        AND (A9.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A9.HD_DEP_COUNT = 4)
                    AND (A9.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A5,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A11
                        INNER JOIN TIME_DIM A12 ON (A11.SS_SOLD_TIME_SK = A12.T_TIME_SK)
                    )
                    INNER JOIN STORE A13 ON (A11.SS_STORE_SK = A13.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A14 ON (A11.SS_HDEMO_SK = A14.HD_DEMO_SK)
            )
        WHERE
            (A12.T_HOUR = 11)
            AND (A12.T_MINUTE < 30)
            AND (A13.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A14.HD_DEP_COUNT = 2)
                        AND (A14.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A14.HD_DEP_COUNT = 1)
                        AND (A14.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A14.HD_DEP_COUNT = 4)
                    AND (A14.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A10,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A16
                        INNER JOIN TIME_DIM A17 ON (A16.SS_SOLD_TIME_SK = A17.T_TIME_SK)
                    )
                    INNER JOIN STORE A18 ON (A16.SS_STORE_SK = A18.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A19 ON (A16.SS_HDEMO_SK = A19.HD_DEMO_SK)
            )
        WHERE
            (A17.T_HOUR = 10)
            AND (30 <= A17.T_MINUTE)
            AND (A18.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A19.HD_DEP_COUNT = 2)
                        AND (A19.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A19.HD_DEP_COUNT = 1)
                        AND (A19.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A19.HD_DEP_COUNT = 4)
                    AND (A19.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A15,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A21
                        INNER JOIN TIME_DIM A22 ON (A21.SS_SOLD_TIME_SK = A22.T_TIME_SK)
                    )
                    INNER JOIN STORE A23 ON (A21.SS_STORE_SK = A23.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A24 ON (A21.SS_HDEMO_SK = A24.HD_DEMO_SK)
            )
        WHERE
            (A22.T_HOUR = 10)
            AND (A22.T_MINUTE < 30)
            AND (A23.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A24.HD_DEP_COUNT = 2)
                        AND (A24.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A24.HD_DEP_COUNT = 1)
                        AND (A24.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A24.HD_DEP_COUNT = 4)
                    AND (A24.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A20,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A26
                        INNER JOIN TIME_DIM A27 ON (A26.SS_SOLD_TIME_SK = A27.T_TIME_SK)
                    )
                    INNER JOIN STORE A28 ON (A26.SS_STORE_SK = A28.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A29 ON (A26.SS_HDEMO_SK = A29.HD_DEMO_SK)
            )
        WHERE
            (A27.T_HOUR = 9)
            AND (30 <= A27.T_MINUTE)
            AND (A28.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A29.HD_DEP_COUNT = 2)
                        AND (A29.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A29.HD_DEP_COUNT = 1)
                        AND (A29.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A29.HD_DEP_COUNT = 4)
                    AND (A29.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A25,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A31
                        INNER JOIN TIME_DIM A32 ON (A31.SS_SOLD_TIME_SK = A32.T_TIME_SK)
                    )
                    INNER JOIN STORE A33 ON (A31.SS_STORE_SK = A33.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A34 ON (A31.SS_HDEMO_SK = A34.HD_DEMO_SK)
            )
        WHERE
            (A32.T_HOUR = 9)
            AND (A32.T_MINUTE < 30)
            AND (A33.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A34.HD_DEP_COUNT = 2)
                        AND (A34.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A34.HD_DEP_COUNT = 1)
                        AND (A34.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A34.HD_DEP_COUNT = 4)
                    AND (A34.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A30,
    (
        SELECT
            COUNT(*) C0
        FROM
            (
                (
                    (
                        STORE_SALES A36
                        INNER JOIN TIME_DIM A37 ON (A36.SS_SOLD_TIME_SK = A37.T_TIME_SK)
                    )
                    INNER JOIN STORE A38 ON (A36.SS_STORE_SK = A38.S_STORE_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A39 ON (A36.SS_HDEMO_SK = A39.HD_DEMO_SK)
            )
        WHERE
            (A37.T_HOUR = 8)
            AND (30 <= A37.T_MINUTE)
            AND (A38.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (A39.HD_DEP_COUNT = 2)
                        AND (A39.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A39.HD_DEP_COUNT = 1)
                        AND (A39.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A39.HD_DEP_COUNT = 4)
                    AND (A39.HD_VEHICLE_COUNT <= 6)
                )
            )
    ) A35
limit
    100;