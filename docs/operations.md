# Эксплуатация и восстановление

Стек рассчитан на одну учебную VM. Репликация Kafka и Garage равна 1;
local-path PVC привязаны к диску этой VM. Пересоздание Pod сохраняет данные,
потеря VM требует внешней резервной копии. Все команды ниже выполняются на VM,
если не сказано иначе. Namespace — `data-stack`.

## Проверка после установки

```bash
kubectl -n data-stack get pods,pvc,jobs
kubectl top nodes
kubectl -n data-stack top pods --containers
df -h /var/lib/rancher/k3s
bash scripts/smoke.sh
```

Smoke проверяет доступность DWH, бакета, топика, конфигурацию Prometheus и ошибки
импорта DAG. Он не расходует квоту внешнего API и не создаёт фиктивных матчей.
Для первой реальной загрузки включите DAG `football_ingestion` в Airflow UI.
Задача `verify` ожидает подтверждение DWH до 180 секунд. Она возвращает в XCom
`expected_count`, `processed_count`, `rejected_count`, `status`.
Пустой ответ имеет статус `empty` и не является ошибкой. `completed_with_errors`
означает отклонённые либо отсутствующие события; смотрите DLQ и историю загрузки.
Записи до миграции 0.3 имеют статус `legacy`: исторические счётчики не выдумываются.
Повтор исходного снимка восстанавливает детализацию обработки без дубликатов ODS.

## Prometheus

Откройте `/targets` и `/alerts` через SSH-туннель из README. Все цели должны быть UP.
Alertmanager не устанавливается: правила видны в UI Prometheus, внешние уведомления
не отправляются. Grafana в проекте отсутствует.

Полезные PromQL-запросы:

```promql
sum(kafka_consumergroup_lag{consumergroup="football-postgres-v1"})
time() - football_last_snapshot_timestamp_seconds
rate(football_events_total[5m])
increase(football_events_total{result="dlq"}[1h])
rate(airflow_scheduler_heartbeat_total[5m])
kube_pod_container_status_last_terminated_reason{reason="OOMKilled"}
```

Kafka Exporter даёт смещения и lag. JVM/JMX-метрики брокера не собираются.
Node Exporter показывает память/диск VM, kube-state-metrics — состояние Pod и Job,
StatsD Exporter — выбранные метрики Airflow. Для остальной диагностики используйте
`kubectl top` и логи. В metrics labels не включаются match ID и имена raw-объектов.

Если `PipelineStale` горит на новом стенде, выполните первую загрузку. После этого
порог — 30 часов. Обычное отсутствие матчей не вызывает этот алерт: пустая успешная
выборка обновляет время получения снимка.

## Проверка восстановления consumer

1. Выполните `kubectl -n data-stack scale deployment/consumer --replicas=0`.
2. Запустите DAG, убедитесь в успешной публикации и росте lag.
3. Верните `kubectl -n data-stack scale deployment/consumer --replicas=1`.
4. Дождитесь уменьшения lag до нуля и проверьте ODS.
5. Повторно опубликуйте тот же raw-объект: новых дубликатов не должно появиться.

В Airflow Trigger DAG → Configuration:

```json
{"raw_key": "raw/football/2026-09-25/<hash>.json.gz"}
```

Ключ можно получить из XCom задачи `snapshot` или `ops.ingestions.raw_object_key`.
Перезапуск `publish` также использует существующий снимок. Файлы старше 30 дней
удаляются; чтобы переигрывать более старую историю, заранее сохраните raw вне VM.

Не увеличивайте число partitions основного топика без изменения протокола
завершения снимка: текущая реализация опирается на порядок в одной partition.

## Ошибочные события

```bash
kubectl -n data-stack exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic football.matches.dlq \
  --from-beginning --max-messages 10 --timeout-ms 10000
```

В DLQ есть исходные topic/partition/offset, причина ошибки, SHA-256 и первые 64 КиБ
исходного сообщения в Base64. Большое сообщение обрезается, чтобы сама отправка
в DLQ не превысила лимит Kafka; это отмечено `payload_truncated`. Для полного
восстановления используйте raw или исходный Kafka offset до истечения retention.
После исправления обработчика повторите публикацию raw-снимка. Ошибочное событие
не записывается в таблицу дедупликации, поэтому исправленный обработчик сможет его
обработать. Недоступность БД не отправляет корректные записи в DLQ.

## Резервные копии

CronJob `database-backup` ежедневно сохраняет `pg_dump -Fc` для `airflow_db`,
`metabase_db` и `dwh_db` в PVC `database-backups`. Завершённый дамп переименовывается
из `.partial`; хранятся несколько последних дней. Это локальные копии для удобного
восстановления, а не защита от потери VM.

Для ручного запуска:

```bash
kubectl -n data-stack create job --from=cronjob/database-backup backup-manual
kubectl -n data-stack wait --for=condition=complete job/backup-manual --timeout=600s
kubectl -n data-stack logs job/backup-manual
```

Перед повторным использованием того же имени удалите **только завершённый** Job
`backup-manual`; PVC при этом не удаляется. Файлы local-path PVC находятся под
`/var/lib/rancher/k3s/storage`. Точный путь можно посмотреть через PVC → PV:

```bash
kubectl -n data-stack get pvc database-backups -o wide
kubectl get pv <PV_NAME> -o yaml
```

Настройте копирование завершённых `.dump` с VM на отдельный диск/сервер своим
backup-инструментом. Адрес внешнего хранилища не задан, поэтому автоматическое
копирование за пределы VM в этой версии не настроено.

Также сохраните Secrets в защищённом месте. Экспорт содержит открытые секреты
в Base64, поэтому файл должен быть доступен только владельцу и зашифрован при
хранении вне VM:

```bash
umask 077
mkdir -p .secrets
kubectl -n data-stack get secret stack-secrets garage-s3 -o yaml > .secrets/credentials.yaml
```

Перед копированием каталога Garage остановите записи и сам Pod либо используйте
согласованный снимок диска. Простое копирование работающей LMDB не является
гарантированно согласованным backup. Более простой вариант raw-backup — выгрузка
объектов S3 вместе с их metadata `fetched-at` через S3-клиент. Нельзя потерять эту
метадату: она нужна для корректного replay.

## Проверка дампа без изменения рабочей БД

Подставьте локальный путь к одному завершённому дампу DWH. Тест использует отдельную
БД `restore_check`; рабочая `dwh_db` не затрагивается:

```bash
kubectl -n data-stack exec postgres-dwh-0 -- createdb -U postgres restore_check
kubectl -n data-stack exec -i postgres-dwh-0 -- \
  pg_restore -U postgres -d restore_check --no-owner --no-privileges < /path/to/dwh_db.dump
kubectl -n data-stack exec postgres-dwh-0 -- \
  psql -U postgres -d restore_check -c 'SELECT count(*) FROM ods.matches;'
```

Затем сравните количество строк и контрольные матчи с исходной БД. После проверки
удалите только тестовую БД `restore_check`. Восстановление Metabase проверяется
аналогично в отдельном экземпляре приложения. Проверка чтения списка файлов дампа
не заменяет настоящий restore.

## Обновление и откат

CI собирает образ один раз, проверяет его и публикует тег commit SHA. CD разрешает
этот тег в digest и развёртывает именно его. `deploy.sh` использует post-install /
post-upgrade Job для создания БД и миграций. `helm --wait` до этой Job намеренно
не используется: на первом запуске это заблокировало бы ожидание Airflow.
После миграций скрипт ждёт готовности workloads и запускает smoke.

При неудаче деплой завершается ошибкой и сохраняет диагностику:

```bash
helm history data-stack -n data-stack
kubectl -n data-stack get pods,jobs
kubectl -n data-stack logs job/stack-migrate
kubectl -n data-stack describe pod <POD_NAME>
```

Если изменение касалось только обратно совместимого прикладного кода, можно
вернуться к предыдущему успешному Helm revision:

```bash
helm rollback data-stack <REVISION> -n data-stack --wait --timeout 10m
bash scripts/smoke.sh
```

Helm не откатывает SQL-миграции, версии метабазы Airflow/Metabase и содержимое PVC.
Для отката обновления Airflow или несовместимой схемы восстановите соответствующий
дамп и прежний образ по отдельному плану. Автоматический откат БД отсутствует.
Применённые SQL-файлы менять нельзя — добавляйте новую миграцию с новым номером.

## Переход с прежнего Compose

Новый chart создаёт новые PVC; Docker volumes автоматически не импортируются.
Старые контейнеры/volumes скрипты не удаляют. До остановки старого стека сохраните
дампы DWH и Metabase, raw-файлы MinIO и необходимые настройки Airflow. На 8 ГБ RAM
не держите одновременно два полных стека.

Рекомендуемый учебный вариант — новая метабаза Airflow 3 и повторное получение
доступных исторических матчей. Airflow 2.9 metadata нельзя просто подставить в
Airflow 3 без официальной процедуры обновления и отдельной проверки.
Parquet из старого MinIO остаётся архивом: новый replay читает JSON + gzip.

Для сохранения старой ODS восстановите дамп в отдельную временную БД и перенесите
записи в новую схему с помощью `python -m pipeline.import_legacy`. Укажите
`LEGACY_DWH_DSN` и обычные `PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD` целевой БД.
`LEGACY_TIMEZONE` по умолчанию UTC, поскольку старый DAG терял timezone при cast
`utcDate` в TIMESTAMP. Проверьте это предположение на ваших данных.
Импорт не перезаписывает уже существующие `match_id`, не переносит метабазу Airflow
и не удаляет старую БД. В Metabase измените подключение источника на `postgres-dwh`
и роль `metabase_reader` после переноса его служебной БД.

Официальные справочники: [обновление Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html),
[Garage](https://garagehq.deuxfleurs.fr/documentation/),
[k3s](https://docs.k3s.io/),
[Metabase](https://www.metabase.com/docs/latest/configuring-metabase/environment-variables).

## Проверяемый backup и копия вне VM

`make backup` создаёт отдельный Job; дождитесь его `Complete` через `make status`.
Затем `make restore-check` восстанавливает последний полный дамп DWH во временную
БД, проверяет таблицы и витрину и удаляет эту БД. Рабочая DWH не заменяется.
При принудительном завершении Job проверьте, не осталась ли БД с префиксом
`restore_check_`: обработчик завершения не исполняется при SIGKILL.

Очистка старых дампов выполняется до нового backup и сохраняет хотя бы последний
полный дамп каждой БД. Порог свободного места — 512 MiB; при больших объёмах
увеличьте `backup.minFreeKiB`. PVC на той же VM не защищает от потери её диска.

На **другом Linux-компьютере**, где настроен вход по SSH-ключу на VM:

```bash
bash scripts/export_backups.sh drn@192.168.225.128 /absolute/path/to/private-backups
```

Скрипт копирует дампы и SHA256-манифесты; повторный запуск обновляет локальную копию.
Планируйте его после ночного backup, например через cron на принимающем компьютере.
Каталог содержит данные приложений: доступ к нему должен быть только у владельца.
Проверьте контрольные суммы последнего полного комплекта через `sha256sum -c`.
Срок хранения внешних копий настраивается отдельно на принимающей стороне.

Для полного восстановления отдельно сохраните Kubernetes Secret `stack-secrets`
в защищённое хранилище: в нём находятся ключи Airflow и Metabase. Не коммитьте его
и не сохраняйте открытым текстом рядом с публичными материалами проекта.
`make restore-check` проверяет DWH; полноценное восстановление Airflow и Metabase
требует отдельного тестового экземпляра соответствующего приложения.

## Ротация API-токена

Запустите `python3 scripts/rotate_api_token.py` на VM с настроенным KUBECONFIG
(и `NAMESPACE`, если он отличается от `data-stack`). Новый токен вводится скрыто;
скрипт меняет только `FOOTBALL_API_KEY` и перезапускает компоненты Airflow.
Пароли PostgreSQL таким способом не ротируются: сначала меняется пароль роли
в самой БД, затем Secret и зависимые приложения в согласованном окне обслуживания.
При первом обновлении создаются стабильные ключи Metabase; повторный вход в UI
после этого обновления ожидаем.

При переходе с незашифрованной Metabase `deploy.sh` выполняет одноразовый
`enable-encryption`: останавливает Metabase, запускает Job с новым ключом из
Secret и возвращает приложение в работу. Bootstrap ставит метку pending только
при первом добавлении ключа в существующий Secret; после успешной операции
метка удаляется. Сначала обязательно сохраните backup.

Не добавляйте эту метку вручную для исправления неверного или утраченного ключа:
нужно восстановить прежний ключ/резервную копию. Повторные деплои не шифруют БД
заново. Основание: [процедура Metabase](https://www.metabase.com/docs/latest/databases/encrypting-details-at-rest).
