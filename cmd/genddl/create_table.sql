CREATE SCHEMA IF NOT EXISTS {{ .Name }}
WITH (
    location = 's3a://presto-workload-v2/{{ .Name }}/'
);
{{ if .Iceberg -}}
USE iceberg.{{ .Name }};
{{ else -}}
USE hive.{{ .Name }};
{{ end -}}

{{ range .Tables -}}
CREATE TABLE IF NOT EXISTS {{ .Name }} (
{{ range .Columns -}}
    {{ .Name }} {{ .Type }},
{{ end -}}
)
{{ end -}}
