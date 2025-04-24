-- Creating nation table with PARQUET format
CREATE TABLE nation (
    nationkey   BIGINT NOT NULL,
    name        VARCHAR(25) NOT NULL,
    regionkey   BIGINT NOT NULL,
    comment     VARCHAR(152) NOT NULL
) WITH (format = 'PARQUET');
