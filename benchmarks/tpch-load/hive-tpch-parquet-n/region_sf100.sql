-- Inserting into region
INSERT INTO region
SELECT
    r_regionkey,
    CAST(r_name AS VARCHAR(25)),
    CAST(r_comment AS VARCHAR(152))
FROM tpch.sf100.region;