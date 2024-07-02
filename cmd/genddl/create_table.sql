CREATE SCHEMA IF NOT EXISTS {{ .Name }}
WITH (
    location = 's3a://presto-workload-v2/{{ .Name }}/'
);
{{ if .Iceberg }}
USE iceberg.{{ .Name }};
{{- else }}
USE hive.{{ .Name }};
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
    location = 's3a://presto-workload-v2/{{ $.Name }}/{{ .Name }}'
)

{{ end -}}
