SELECT
    MAX(
        CASE
            WHEN (A0.C0 = 1) THEN A0.C1
            ELSE NULL
        END
    ) "H8_30_TO_9",
    MAX(
        CASE
            WHEN (A0.C0 = 2) THEN A0.C1
            ELSE NULL
        END
    ) "H9_TO_9_30",
    MAX(
        CASE
            WHEN (A0.C0 = 3) THEN A0.C1
            ELSE NULL
        END
    ) "H9_30_TO_10",
    MAX(
        CASE
            WHEN (A0.C0 = 4) THEN A0.C1
            ELSE NULL
        END
    ) "H10_TO_10_30",
    MAX(
        CASE
            WHEN (A0.C0 = 5) THEN A0.C1
            ELSE NULL
        END
    ) "H10_30_TO_11",
    MAX(
        CASE
            WHEN (A0.C0 = 6) THEN A0.C1
            ELSE NULL
        END
    ) "H11_TO_11_30",
    MAX(
        CASE
            WHEN (A0.C0 = 7) THEN A0.C1
            ELSE NULL
        END
    ) "H11_30_TO_12",
    MAX(
        CASE
            WHEN (A0.C0 = 8) THEN A0.C1
            ELSE NULL
        END
    ) "H12_TO_12_30"
FROM
    (
        SELECT
            CASE
                WHEN (
                    (A1.C0 = 1)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 8)
                    )
                ) THEN 1
                WHEN (
                    (A1.C0 = 2)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 9)
                    )
                ) THEN 2
                WHEN (
                    (A1.C0 = 3)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 9)
                    )
                ) THEN 3
                WHEN (
                    (A1.C0 = 4)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 10)
                    )
                ) THEN 4
                WHEN (
                    (A1.C0 = 5)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 10)
                    )
                ) THEN 5
                WHEN (
                    (A1.C0 = 6)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 11)
                    )
                ) THEN 6
                WHEN (
                    (A1.C0 = 7)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 11)
                    )
                ) THEN 7
                WHEN (
                    (A1.C0 = 8)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 12)
                    )
                ) THEN 8
                ELSE NULL
            END C0,
            COUNT(
                CASE
                    WHEN (
                        (A1.C0 = 1)
                        AND (
                            (30 <= A4.T_MINUTE)
                            AND (A4.T_HOUR = 8)
                        )
                    ) THEN 1
                    WHEN (
                        (A1.C0 = 2)
                        AND (
                            (A4.T_MINUTE < 30)
                            AND (A4.T_HOUR = 9)
                        )
                    ) THEN 2
                    WHEN (
                        (A1.C0 = 3)
                        AND (
                            (30 <= A4.T_MINUTE)
                            AND (A4.T_HOUR = 9)
                        )
                    ) THEN 3
                    WHEN (
                        (A1.C0 = 4)
                        AND (
                            (A4.T_MINUTE < 30)
                            AND (A4.T_HOUR = 10)
                        )
                    ) THEN 4
                    WHEN (
                        (A1.C0 = 5)
                        AND (
                            (30 <= A4.T_MINUTE)
                            AND (A4.T_HOUR = 10)
                        )
                    ) THEN 5
                    WHEN (
                        (A1.C0 = 6)
                        AND (
                            (A4.T_MINUTE < 30)
                            AND (A4.T_HOUR = 11)
                        )
                    ) THEN 6
                    WHEN (
                        (A1.C0 = 7)
                        AND (
                            (30 <= A4.T_MINUTE)
                            AND (A4.T_HOUR = 11)
                        )
                    ) THEN 7
                    WHEN (
                        (A1.C0 = 8)
                        AND (
                            (A4.T_MINUTE < 30)
                            AND (A4.T_HOUR = 12)
                        )
                    ) THEN 8
                    ELSE NULL
                END
            ) C1
        FROM
            (
                VALUES
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8
            ) A1(C0),
            (
                (
                    (
                        STORE_SALES A2
                        INNER JOIN STORE A3 ON (A2.SS_STORE_SK = A3.S_STORE_SK)
                    )
                    INNER JOIN TIME_DIM A4 ON (A2.SS_SOLD_TIME_SK = A4.T_TIME_SK)
                )
                INNER JOIN HOUSEHOLD_DEMOGRAPHICS A5 ON (A2.SS_HDEMO_SK = A5.HD_DEMO_SK)
            )
        WHERE
            (A3.S_STORE_NAME = 'ese')
            AND (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (30 <= A4.T_MINUTE)
                                            AND (A4.T_HOUR = 8)
                                        )
                                        OR (
                                            (A4.T_MINUTE < 30)
                                            AND (A4.T_HOUR = 9)
                                        )
                                    )
                                    OR (
                                        (30 <= A4.T_MINUTE)
                                        AND (A4.T_HOUR = 9)
                                    )
                                )
                                OR (
                                    (A4.T_MINUTE < 30)
                                    AND (A4.T_HOUR = 10)
                                )
                            )
                            OR (
                                (30 <= A4.T_MINUTE)
                                AND (A4.T_HOUR = 10)
                            )
                        )
                        OR (
                            (A4.T_MINUTE < 30)
                            AND (A4.T_HOUR = 11)
                        )
                    )
                    OR (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 11)
                    )
                )
                OR (
                    (A4.T_MINUTE < 30)
                    AND (A4.T_HOUR = 12)
                )
            )
            AND (
                (
                    (
                        (A5.HD_DEP_COUNT = 2)
                        AND (A5.HD_VEHICLE_COUNT <= 4)
                    )
                    OR (
                        (A5.HD_DEP_COUNT = 1)
                        AND (A5.HD_VEHICLE_COUNT <= 3)
                    )
                )
                OR (
                    (A5.HD_DEP_COUNT = 4)
                    AND (A5.HD_VEHICLE_COUNT <= 6)
                )
            )
        GROUP BY
            CASE
                WHEN (
                    (A1.C0 = 1)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 8)
                    )
                ) THEN 1
                WHEN (
                    (A1.C0 = 2)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 9)
                    )
                ) THEN 2
                WHEN (
                    (A1.C0 = 3)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 9)
                    )
                ) THEN 3
                WHEN (
                    (A1.C0 = 4)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 10)
                    )
                ) THEN 4
                WHEN (
                    (A1.C0 = 5)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 10)
                    )
                ) THEN 5
                WHEN (
                    (A1.C0 = 6)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 11)
                    )
                ) THEN 6
                WHEN (
                    (A1.C0 = 7)
                    AND (
                        (30 <= A4.T_MINUTE)
                        AND (A4.T_HOUR = 11)
                    )
                ) THEN 7
                WHEN (
                    (A1.C0 = 8)
                    AND (
                        (A4.T_MINUTE < 30)
                        AND (A4.T_HOUR = 12)
                    )
                ) THEN 8
                ELSE NULL
            END
    ) A0
limit
    100;