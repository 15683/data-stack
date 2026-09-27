# Дашборд Football overview

Пройдите первоначальную настройку Metabase. Создайте источник PostgreSQL:
host `postgres-dwh`, port `5432`, database `dwh_db`, user `metabase_reader`, SSL off
внутри учебного namespace. Пароль — поле `METABASE_READER_PASSWORD` Secret stack-secrets.
Для demo используйте Secret в data-stack-demo. Это отдельный экземпляр Metabase.

После обновления выполните Admin → Databases → DWH → Sync database schema.
Создайте коллекцию **Football lab** и четыре SQL-вопроса из `analytics/`:

| Файл | Название | Визуализация |
|---|---|---|
| matches.sql | Matches and goals | Таблица или отдельные числовые карточки |
| teams.sql | Goals by team | Горизонтальные столбцы: team_name / goals_for |
| loads.sql | Recent loads | Таблица, status выделить цветом |
| freshness.sql | Last successful fetch | Одно значение времени |

`[[...]]` и `{{season}}` — синтаксис необязательного числового параметра Metabase.
В SQL-редакторе задайте переменной season тип Number. На дашборде добавьте фильтр
**Season start year** и свяжите его с параметрами первых двух вопросов.
Для demo: сезон 2026, 3 матча, 9 голов, Owls — 7, Foxes — 1, Bears — 1.
Последние загрузки и время получения не фильтруются сезоном, чтобы диагностика
не скрывала новые ошибки. Старые матчи с NULL сезоном видны без фильтра.

Дашборд восстанавливается из SQL и этой инструкции без платной сериализации.
Учётную запись владельца и пароли Metabase не храните в репозитории.

## Скриншоты для GitHub

После загрузки demo и создания dashboard сохраните три снимка в `docs/screenshots/`:
`metabase-demo.png` (3 матча, 9 голов и распределение по командам),
`airflow-success.png` (успешные snapshot/publish/verify),
`prometheus-targets.png` (доступность целей мониторинга).
Снимайте реальный интерфейс после проверки результата. Не включайте страницы
с паролями, токенами или содержимое Secret. Обновляйте изображения при изменении
витрин. Скриншоты не включены в текущую проверку: обновлённый стенд ещё не развёрнут.
