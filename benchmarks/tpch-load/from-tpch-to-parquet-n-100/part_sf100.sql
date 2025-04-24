-- Inserting into part
INSERT INTO part
SELECT
    p_partkey,
    CAST(p_name AS VARCHAR(55)),
    CAST(p_mfgr AS VARCHAR(25)),
    CAST(p_brand AS VARCHAR(10)),
    CAST(p_type AS VARCHAR(25)),
    p_size,
    CAST(p_container AS VARCHAR(10)),
    CAST(p_retailprice AS DECIMAL(12,2)),
    CAST(p_comment AS VARCHAR(23))
FROM tpch.sf100.part;
