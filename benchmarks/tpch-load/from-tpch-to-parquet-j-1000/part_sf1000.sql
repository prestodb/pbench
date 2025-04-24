-- Inserting into part
INSERT INTO part
SELECT
    partkey,
    CAST(name AS VARCHAR(55)),
    CAST(mfgr AS VARCHAR(25)),
    CAST(brand AS VARCHAR(10)),
    CAST(type AS VARCHAR(25)),
    size,
    CAST(container AS VARCHAR(10)),
    CAST(retailprice AS DECIMAL(12,2)),
    CAST(comment AS VARCHAR(23))
FROM tpch.sf1000.part;
