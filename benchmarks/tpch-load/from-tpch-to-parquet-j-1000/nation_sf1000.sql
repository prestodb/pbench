-- Inserting into nation
INSERT INTO nation
SELECT
    nationkey,
    CAST(name AS VARCHAR(25)),
    regionkey,
    CAST(comment AS VARCHAR(152))
FROM tpch.sf1000.nation;
