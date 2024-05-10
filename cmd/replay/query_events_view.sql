CREATE VIEW
    query_events_view AS
SELECT
    dt date,
    json_extract_scalar (record, '$.clusterName') cluster_name,
    json_extract_scalar (record, '$.queryCompletedEvent.metadata.queryId') query_id,
    from_unixtime (
        CAST(
            json_extract_scalar (
                record,
                '$.queryCompletedEvent.createTime'
            ) AS double
        ), 'UTC'
    ) create_time,
    json_extract_scalar (record, '$.wallTimeMillis') wallTimeMillis,
    json_extract_scalar (
        record,
        '$.queryCompletedEvent.statistics.outputRows'
    ) output_rows,
    json_extract_scalar (
        record,
        '$.queryCompletedEvent.statistics.writtenOutputRows'
    ) written_output_rows,
    json_extract_scalar (record, '$.queryCompletedEvent.context.catalog') catalog,
    json_extract_scalar (record, '$.queryCompletedEvent.context.schema') schema,
    CAST(
        json_extract (
            record,
            '$.queryCompletedEvent.context.sessionProperties'
        ) AS MAP (VARCHAR, VARCHAR)
    ) session_properties,
    json_extract_scalar (record, '$.queryCompletedEvent.metadata.query') query
FROM
    query_events_raw
WHERE
    (
        json_extract (record, '$.queryCompletedEvent') <> JSON 'null'
    )
ORDER BY
    create_time;