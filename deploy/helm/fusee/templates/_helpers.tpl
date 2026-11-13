{{- define "fusee.name" -}}{{ .Release.Name }}-fusee{{- end -}}
{{- define "fusee.labels" -}}
app.kubernetes.io/name: fusee
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "fusee.replicas" -}}
{{- $root := . -}}
{{- $out := list -}}
{{- range $i := until (int .Values.members) -}}
{{- $out = append $out (printf "%s-%d.%s-members:%d" (include "fusee.name" $root) $i (include "fusee.name" $root) (int $root.Values.ports.replication)) -}}
{{- end -}}
{{- join "," $out -}}
{{- end -}}
