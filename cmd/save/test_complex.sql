set session hive.collect_column_statistics_on_write=false;

-- map
create table
    tmap (
        c1 map (
            varchar,
            row (
                "string_value" varchar,
                "int_value" bigint,
                "float_value" double,
                "double_value" double
            )
        ),
        c2 int
    ) WITH (format = 'PARQUET');

INSERT INTO tmap VALUES
    (map(
        array['A', 'B'],
        array[ROW('a', 3, 4.0, 5.0), ROW('b', 6, 7.0, 8.0)]
    ), 1),
    (map(
        array['C', 'D'],
        array[ROW('a2', 3, 4.0, 5.0), ROW('b2', 6, 7.0, 8.0)]
    ), 2),
    (map(
        array['F', 'D'],
        array[ROW(null, null, null, 5.0), ROW('b2', 6, 7.0, 8.0)]
    ), null);

-- array
create table tarray(c1 array(row("name" varchar, "comment" varchar, "hint" varchar, "score" bigint, "tags" array(varchar))));

INSERT INTO tarray VALUES
    (array[
        ROW('a', 'a2', 'a3', 3, array['a4', 'a5', 'a6']),
        ROW('b', 'b2', 'b3', 4, array['b4'])]
    ),
    (array[
        ROW('aa', 'a2', 'a3', 3, array['aa4']),
        ROW('bb', 'b2', 'b3', 4, array['b4'])]
    );

-- row
create table trow(c1  row("string_value" varchar, "int_value" bigint, "float_value" double, "double_value" double));

INSERT INTO trow VALUES (row(row('a', 3, 4.0, 5.0))), (row(row('aa', 5, 6.0, 7.0)));