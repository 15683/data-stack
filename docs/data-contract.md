# Контракт событий v1

Топик: `football.matches.v1`, одна partition. JSON UTF-8. Kafka key матча — его ID.

Обязательные поля: `schema_version=1`, `kind`, `event_id`, `raw_object_key`,
`fetched_at` с timezone. Для `kind=match` обязателен объект `match` с положительным
32-битным `id` и `utcDate`. Для `snapshot_complete` — неотрицательный `match_count`.

Добавочные необязательные поля совместимы с v1:

```json
{
  "context": {
    "run_id": "manual__example",
    "date_from": "2026-01-01",
    "date_to": "2026-01-05",
    "competition": "DEMO",
    "source_mode": "demo"
  }
}
```

Старые события без context читаются. season.id/startDate/endDate необязательны;
старые строки остаются с NULL сезоном до повторной загрузки. Название команды —
отображаемое значение; ключ группировки — team_id, competition_code и сезон.

event_id = SHA256(raw_object_key + ':' + match_id); для завершения вместо ID используется
`complete`. Изменение обязательных полей или смысла идентификатора требует новой
версии контракта и плана миграции. JSON schema в коде реализована явными валидаторами;
Schema Registry для такого объёма не нужен.

В DLQ: причина, topic/partition/offset, SHA256, исходный размер и первые 64 KiB
сообщения в Base64. Секреты и токены в события не включаются. Не добавляйте match_id
и raw_key в Prometheus labels: используйте структурированные логи/SQL для детализации.
