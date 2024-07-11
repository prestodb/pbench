{{ range $key, $value := .SessionVariables -}}
SET SESSION {{ $key }}='{{ $value }}';
{{ end }}
CREATE SCHEMA IF NOT EXISTS {{ if .Iceberg }}iceberg.{{ .SchemaName }};
{{- else }}hive.{{ .SchemaName }};
{{- end }}
WITH (
    location = 's3a://presto-workload-v2/{{ .LocationName }}/'
);
{{ if .Iceberg }}
USE iceberg.{{ .SchemaName }};
{{- else }}
USE hive.{{ .SchemaName }};
{{- end }}

{{- if .RegisterTables }}
{{ range .RegisterTables }}
CALL iceberg.system.register_table('{{ $.SchemaName }}', '{{ .TableName }}', 's3a://presto-workload-v2/{{ .ExternalLocation }}/{{ .TableName }}/metadata');
{{- end }}
{{- end }}

{{ range .Tables -}}
CREATE TABLE IF NOT EXISTS {{ .Name }} (
{{- $first := true }}
{{- range .Columns }}
    {{- if $first }}
        {{- $first = false }}
    {{- else -}}
        ,
    {{- end }}
    {{ .Name }} {{ .Type }}
{{- end }}
)
WITH (
    format = 'PARQUET',
    {{- if $.Partitioned }}
    {{- if $.Iceberg}}
    partitioning = array['{{ .LastColumn.Name }}']
    {{- else if .Partitioned }}
    partitioned_by = array['{{ .LastColumn.Name }}']
    {{- else }}
    external_location = 's3a://presto-workload-v2/{{ $.IcebergLocationName }}/{{ .Name }}/data/'
    {{- end }}
    {{- else if $.Iceberg }}
    location = 's3a://presto-workload-v2/{{ $.LocationName }}/{{ .Name }}'
    {{- else }}
    external_location = 's3a://presto-workload-v2/{{ $.IcebergLocationName }}/{{ .Name }}/data/'
    {{- end }}
);

{{ end -}}
