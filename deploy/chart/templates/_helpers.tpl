{{- define "stack.labels" -}}
app.kubernetes.io/part-of: data-stack
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "stack.pullSecrets" -}}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "stack.pipelineEnv" -}}
- name: SOURCE_MODE
  value: {{ .Values.sourceMode | quote }}
- name: STACK_NAMESPACE
  value: {{ .Release.Namespace | quote }}
- name: FOOTBALL_COMPETITION
  value: {{ .Values.footballCompetition | quote }}
- name: S3_ENDPOINT_URL
  value: http://garage:3900
- name: S3_BUCKET
  value: data-stack
- name: KAFKA_BOOTSTRAP_SERVERS
  value: kafka:9092
- name: PGHOST
  value: postgres-dwh
- name: PGDATABASE
  value: dwh_db
- name: PGUSER
  value: dwh_writer
- name: PGPASSWORD
  valueFrom:
    secretKeyRef: {name: stack-secrets, key: DWH_PASSWORD}
{{- end }}

{{- define "stack.airflowEnv" -}}
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  valueFrom:
    secretKeyRef: {name: stack-secrets, key: AIRFLOW_DB_URI}
- name: AIRFLOW__CORE__FERNET_KEY
  valueFrom:
    secretKeyRef: {name: stack-secrets, key: AIRFLOW_FERNET_KEY}
- name: AIRFLOW__API_AUTH__JWT_SECRET
  valueFrom:
    secretKeyRef: {name: stack-secrets, key: AIRFLOW_JWT_SECRET}
- name: AIRFLOW__API__SECRET_KEY
  valueFrom:
    secretKeyRef: {name: stack-secrets, key: AIRFLOW_API_SECRET}
- name: AIRFLOW__CORE__EXECUTOR
  value: LocalExecutor
- name: AIRFLOW__CORE__AUTH_MANAGER
  value: airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
- name: AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS
  value: admin:admin
- name: AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE
  value: /opt/airflow/auth/passwords.json
- name: AIRFLOW__CORE__EXECUTION_API_SERVER_URL
  value: http://airflow-api:8080/execution/
- name: AIRFLOW__CORE__LOAD_EXAMPLES
  value: "false"
- name: AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION
  value: "true"
- name: AIRFLOW__CORE__PARALLELISM
  value: "1"
- name: AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG
  value: "1"
- name: AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG
  value: "1"
- name: AIRFLOW__API__WORKERS
  value: "1"
- name: AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES
  value: "1"
- name: AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL
  value: "120"
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE
  value: "3"
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW
  value: "2"
- name: AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK
  value: "true"
- name: AIRFLOW__LOGGING__LOGGING_LEVEL
  value: INFO
- name: AIRFLOW__METRICS__STATSD_ON
  value: "true"
- name: AIRFLOW__METRICS__STATSD_HOST
  value: statsd
- name: AIRFLOW__METRICS__STATSD_PORT
  value: "9125"
- name: AIRFLOW__METRICS__STATSD_PREFIX
  value: airflow
{{- end }}
