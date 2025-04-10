-- Inserting into region
INSERT INTO region
SELECT
    regionkey,
    CAST(name AS VARCHAR(25)),
    CAST(comment AS VARCHAR(152))
FROM hive.tpch_sf10_parquet.region;