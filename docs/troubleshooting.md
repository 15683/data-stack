# Диагностика

| Симптом | Что проверить / сделать |
|---|---|
| Нет Pod после deploy | `make doctor`; ссылки образов, namespace, ошибка предыдущего этапа |
| ImagePullBackOff для local-* | `make import-images`; Docker и k3s имеют разные хранилища |
| Kafka CrashLoopBackOff | `kubectl logs kafka-0`; числовые параметры должны быть десятичными строками |
| После исправления Kafka осталась старая ревизия | Сравнить currentRevision/updateRevision. При зависшем rollout пересоздать только kafka-0, сохранив PVC |
| StatsD не стартует | Проверить mapping через `python scripts/check_statsd.py` |
| Metabase падает | Проверить OOMKilled и Java Metaspace; сохранять резерв между JVM и memory limit |
| raw_key обязателен | В кластере старый образ; собрать, импортировать, развернуть и дождаться DAG processor |
| DAG empty / таблицы пусты | Проверить период и соревнование; для гарантированного примера используйте demo |
| verify timeout | Проверить consumer, lag, DLQ и `ops.ingestions`; при replay нужен существующий raw |
| SSH конфликтует с портом 1080 | Launcher использует `-F NUL`, игнорируя чужие SSH forwards |
| UI-порт занят | Остановить предыдущий туннель, запускать один launcher |
| Backup не создался | Проверить свободный диск, Job logs; последний завершённый дамп не удалять |

```bash
kubectl -n data-stack get pods,jobs
kubectl -n data-stack get events --sort-by=.lastTimestamp | tail -30
kubectl -n data-stack logs deployment/consumer --tail=80
make status
```

Для Airflow смотрите task logs в UI. При обращении за помощью приложите этап
deploy, имя образа и текст ошибки. Токены, пароли и содержимое Secret не публикуйте.
