{{ range $key, $value := .SessionVariables -}}
SET SESSION {{ $key }}='{{ $value }}';
{{ end }}

{{- if .Iceberg }}
USE iceberg.{{ .SchemaName }};
{{- else }}
USE hive.{{ .SchemaName }};
{{- end }}

{{ range .Tables -}}
INSERT INTO {{ .Name }}
{{- if eq $.CompressionMethod "uncompressed" }}
SELECT {{- $first := true }}
{{- range .Columns }}
    {{- if $first }}
        {{- $first = false }}
    {{- else -}}
        ,
    {{- end }}
    cast({{ .Name }} as {{ .Type }})
{{- end }}
FROM tpcds.sf{{ $.ScaleFactor }}.{{ .Name }};
{{ else }}
SELECT * FROM {{ if $.Iceberg }}iceberg{{ else }}hive{{ end }}.{{ $.UncompressedName }}.{{ .Name }};
{{- end }}

{{ end }}
{{- range .Tables -}}
ANALYZE {{ .Name }};
{{ end -}}
