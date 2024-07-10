{{ range $key, $value := .SessionVariables -}}
SET SESSION {{ $key }}='{{ $value }}';
{{ end }}

{{ if .Iceberg }}
USE iceberg.{{ .Name }};
{{- else }}
USE hive.{{ .Name }};
{{- end }}

{{ range .Tables -}}
INSERT INTO {{ .Name }} 
SELECT {{- $first := true }}
{{- range .Columns }}
    {{- if $first }}
        {{- $first = false }}
    {{- else -}}
        ,
    {{- end }}
    cast({{ .Name }} as {{ .Type }}
{{- end }}
FROM tpcds.sf{{ $.ScaleFactor }}.{{ .Name }};

{{ end }}

