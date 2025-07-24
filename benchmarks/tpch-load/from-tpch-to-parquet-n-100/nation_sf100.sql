-- Inserting into nation
INSERT INTO nation
SELECT
    n_nationkey,
    CAST(n_name AS VARCHAR(25)),
    n_regionkey,
    CAST(n_comment AS VARCHAR(152))
FROM tpch.sf100.nation;
