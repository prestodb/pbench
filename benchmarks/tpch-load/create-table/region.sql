-- Creating region table with PARQUET format
CREATE TABLE region (
    regionkey   BIGINT NOT NULL,
    name        VARCHAR(25) NOT NULL,
    comment     VARCHAR(152) NOT NULL
) WITH (format = 'PARQUET');