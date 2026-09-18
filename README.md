
# ⚽ Football Data Pipeline

Учебный ELT-пайплайн, который каждый день забирает статистику матчей Лиги чемпионов УЕФА из публичного API, складывает сырые данные в S3-совместимое хранилище и загружает их в аналитическую БД для визуализации в BI.

Проект демонстрирует Raw → ODS архитектуру на минимальном, но полностью рабочем стеке: **Airflow, MinIO, DuckDB, PostgreSQL, Metabase** — всё поднимается одной командой `docker compose up`.

![Python](https://img.shields.io/badge/python-3.11-blue?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-1.2.2-FFF000?logo=duckdb&logoColor=black)
![MinIO](https://img.shields.io/badge/MinIO-S3--compatible-C72E49?logo=minio&logoColor=white)
![Metabase](https://img.shields.io/badge/Metabase-BI-509EE3?logo=metabase&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

## Оглавление

- [О проекте](#о-проекте)
- [Архитектура](#архитектура)
- [Технологический стек](#технологический-стек)
- [Как устроен пайплайн](#как-устроен-пайплайн)
- [Структура репозитория](#структура-репозитория)
- [Быстрый старт](#быстрый-старт)
- [Схема данных](#схема-данных)
- [Порты и доступы](#порты-и-доступы)
- [Скриншоты](#скриншоты)
- [Продакшн](#продакшн)
- [Возможные улучшения](#возможные-улучшения)
- [Лицензия](#лицензия)
- [Автор](#автор)

## О проекте

Источник данных — [football-data.org](https://www.football-data.org/) (competition code `CL`, [UEFA Champions League](https://native-stats.org/competition/CL/)). Пайплайн реализует классическое разделение на слои:

- **Raw** — сырой ответ API как есть, в формате Parquet, в MinIO;
- **ODS** — очищенные, типизированные данные о матчах в PostgreSQL, готовые для BI.

Весь ETL/ELT выполняется без внешних оркестраторов трансформации (dbt, Spark и т.п.) — DuckDB используется как лёгкий движок, который одним SQL-запросом читает Parquet из S3 и пишет напрямую в Postgres через `postgres`-расширение.

## Архитектура

![Архитектура пайплайна: API → Airflow → MinIO (Raw) → DuckDB → PostgreSQL (ODS/DM) → Metabase](https://github.com/15683/data-stack/raw/main/images/flow.png)

## Технологический стек

| Компонент | Технология | Роль в проекте |
|---|---|---|
| Оркестрация | Apache Airflow 2.9.1 (LocalExecutor) | Планирование и запуск DAG'ов, data-aware триггер между пайплайнами через Airflow Datasets |
| Raw-хранилище | MinIO (S3-совместимое) | Сырые данные API в формате Parquet |
| Трансформация | DuckDB 1.2.2 (`httpfs` + `postgres` extensions) | Чтение Parquet из S3 и upsert в Postgres одним SQL-запросом, без отдельного ETL-сервиса |
| ODS / DWH | PostgreSQL 16 (отдельный контейнер `postgres_dwh`) | Слой очищенных данных о матчах |
| BI | Metabase | Дашборды поверх ODS-слоя |
| Инфраструктура | Docker Compose | Локальный и серверный запуск всего стека одной командой |

## Как устроен пайплайн

В проекте два DAG'а, связанные не расписанием, а **Airflow Dataset** — второй DAG стартует автоматически сразу после успешного завершения первого, без сенсоров и поллинга:

![Зависимость DAG'ов через Airflow Dataset: raw_football_matches_from_api_to_s3 → s3://data-stack/raw/football → ods_football_matches_from_s3_to_pg](https://github.com/15683/data-stack/raw/main/images/dag_dep.png)

**1. `raw_football_matches_from_api_to_s3`** — `dags/football_raw_api_to_s3.py`
- `schedule_interval=None` — запускается только вручную/по требованию;
- забирает список матчей CL из football-data.org за логическую дату запуска;
- сохраняет ответ в MinIO: `s3://data-stack/raw/football/{date}/CL_matches.parquet`;
- по завершении публикует Dataset `s3://data-stack/raw/football`, который триггерит второй DAG.

**2. `ods_football_matches_from_s3_to_pg`** — `dags/football_raw_s3_to_pg.py`
- `schedule=[Dataset("s3://data-stack/raw/football")]` — запускается автоматически;
- через DuckDB подключается к MinIO (`httpfs`) и к `postgres_dwh` (`postgres` extension) в одной сессии;
- читает Parquet, переименовывает поля в snake_case и делает `INSERT ... ON CONFLICT (match_id) DO UPDATE` в `ods.matches` — повторный запуск за тот же день идемпотентен.

## Структура репозитория

```
data-stack/
├── dags/
│   ├── football_raw_api_to_s3.py   # DAG 1: API → MinIO (Raw layer)
│   └── football_raw_s3_to_pg.py    # DAG 2: MinIO → PostgreSQL (ODS layer), через DuckDB
├── images/                         # Диаграммы и скриншоты для README
├── docker-compose.yml              # Airflow, 2×PostgreSQL, MinIO, Metabase
├── requirements.txt                # Python-зависимости для локальной разработки/IDE
├── LICENSE                         # MIT
└── README.md
```

## Быстрый старт

### Предварительные требования

- Docker Engine + Docker Compose plugin;
- свободные ~4 ГБ RAM и порты из раздела [«Порты и доступы»](#порты-и-доступы);
- токен API с [football-data.org](https://www.football-data.org/client/register) (бесплатный тариф).

### 1. Клонировать репозиторий

```bash
git clone https://github.com/15683/data-stack.git
cd data-stack
```

### 2. Поднять инфраструктуру

```bash
docker compose up -d
```

При первом запуске это займёт несколько минут: `airflow-init` инициализирует метабазу и создаёт пользователя `admin`/`admin`, а в образы Airflow дополнительно ставятся `duckdb`, `requests`, `pandas`, `s3fs`, `pyarrow` (через `_PIP_ADDITIONAL_REQUIREMENTS`).

### 3. Задать Airflow Variables

Откройте `http://localhost:8080` (`admin` / `admin`) → **Admin → Variables** и добавьте:

| Key | Значение по умолчанию в `docker-compose.yml` | Назначение |
|---|---|---|
| `football_api_key` | — (получить самостоятельно) | Авторизация в football-data.org API |
| `access_key` | `minioadmin` | MinIO access key (`MINIO_ROOT_USER`) |
| `secret_key` | `minio_password` | MinIO secret key (`MINIO_ROOT_PASSWORD`) |
| `pg_password` | `dwh_password` | Пароль пользователя `dwh_user` в `postgres_dwh` |

> ⚠️ Это dev-креды из `docker-compose.yml`, захардкоженные для локального запуска — для любого окружения, доступного извне, их нужно заменить.

### 4. Создать таблицу в DWH

Второй DAG выполняет `INSERT ... ON CONFLICT (match_id)` в `ods.matches` — таблица должна существовать заранее. Подключитесь к `postgres_dwh` (`localhost:5433`, `dwh_user`/`dwh_password`, БД `dwh_db`) и выполните DDL из раздела [«Схема данных»](#схема-данных).

### 5. Запустить пайплайн

В Airflow UI включите (unpause) оба DAG'а и запустите `raw_football_matches_from_api_to_s3` вручную (**Trigger DAG**). После успешного завершения `ods_football_matches_from_s3_to_pg` запустится сам — через Dataset.

### 6. Подключить Metabase

Откройте `http://localhost:3001` и пройдите мастер первичной настройки (служебная БД самого Metabase — `metabase_db` внутри контейнера `postgres`, см. `docker-compose.yml`). Затем добавьте отдельное подключение источника данных к `postgres_dwh` (host `postgres_dwh`, порт `5432` внутри Docker-сети, БД `dwh_db`) и стройте дашборды поверх `ods.matches`.

## Схема данных

`ods.matches` в репозитории не выражена отдельной SQL-миграцией — ниже DDL, восстановленный по `INSERT`-запросу из `dags/football_raw_s3_to_pg.py`; его стоит выполнить вручную перед первым запуском (см. шаг 4) или со временем вынести в отдельный init-скрипт:

```sql
CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.matches (
    match_id             INTEGER PRIMARY KEY,
    utc_date              TIMESTAMP,
    status                VARCHAR,
    matchday              INTEGER,
    stage                 VARCHAR,
    match_group           VARCHAR,
    home_team_id          INTEGER,
    home_team_name        VARCHAR,
    away_team_id          INTEGER,
    away_team_name        VARCHAR,
    winner                VARCHAR,
    score_full_time_home  INTEGER,
    score_full_time_away  INTEGER,
    score_half_time_home  INTEGER,
    score_half_time_away  INTEGER,
    competition_code      VARCHAR,
    competition_name      VARCHAR,
    load_ts               TIMESTAMP
);
```

## Порты и доступы

| Сервис | Порт на хосте | Назначение |
|---|---|---|
| Airflow Webserver | `8080` | UI Airflow (`admin` / `admin` по умолчанию) |
| PostgreSQL (Airflow) | `5432` | Метабаза Airflow, БД `airflow_db` |
| PostgreSQL (DWH) | `5433` | ODS-слой, БД `dwh_db` |
| MinIO API | `9000` | S3-совместимый API |
| MinIO Console | `9001` | Веб-консоль (`minioadmin` / `minio_password`) |
| Metabase | `3001` → `3000` | BI-интерфейс |

## Скриншоты

**DAG'и в Airflow:**

![Список DAG'ов в Airflow UI](https://github.com/15683/data-stack/raw/main/images/dag.png)

**Raw-слой в MinIO:**

![Parquet-файл с сырыми данными матчей в MinIO Object Browser](https://github.com/15683/data-stack/raw/main/images/minio.png)

**Дашборд в Metabase:**

![Дашборд Metabase: топ-10 команд по забитым голам в Лиге чемпионов](https://github.com/15683/data-stack/raw/main/images/bi.png)

## Продакшн

Стек в текущем виде (без изменений в `docker-compose.yml`) развёрнут на VPS ruvds.com (2×2.2 ГГц, 7 ГБ RAM, 70 ГБ SSD RAID).

## Лицензия

Проект распространяется по лицензии **MIT** — см. [LICENSE](LICENSE).

## Автор

**[15683](https://github.com/15683)**
