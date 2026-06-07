{{- define "responses-api-store.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "responses-api-store.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "responses-api-store.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "responses-api-store.labels" -}}
helm.sh/chart: {{ include "responses-api-store.chart" . }}
{{ include "responses-api-store.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "responses-api-store.selectorLabels" -}}
app.kubernetes.io/name: {{ include "responses-api-store.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "responses-api-store.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "responses-api-store.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "responses-api-store.redisUrl" -}}
{{- if .Values.redis.url -}}
{{- .Values.redis.url -}}
{{- else if .Values.valkey.enabled -}}
{{- printf "redis://%s-valkey:%d" (include "responses-api-store.fullname" .) (.Values.valkey.service.port | int) -}}
{{- else -}}
{{- fail "redis.url must be set when valkey.enabled is false" -}}
{{- end -}}
{{- end -}}