# Локальный запуск без CI/CD

Целевая машина: Ubuntu 22.04, 4 vCPU, 8 GiB RAM, диск 80 GB. Выполняйте команды
из корня проекта. Установите Docker (для сборки), Helm 3.22.0, make и python3.
Для установленного k3s повторять установку не нужно. Для чистой VM:

```bash
df -h /
free -h
swapon --show
# Отключите swap и его автоматическое подключение перед установкой k3s.
sudo bash scripts/install-k3s.sh
mkdir -p "$HOME/.kube"
sudo install -m 600 -o "$(id -u)" -g "$(id -g)" /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
export KUBECONFIG="$HOME/.kube/config"
kubectl get nodes
```

Docker должен быть доступен пользователю (`docker info`). Настройка доступа к
Docker даёт административные возможности на машине; используйте свою учебную VM.
VM должна иметь исходящий доступ к registry и, для live-режима, football-data.org.

## Сохранённые настройки

```bash
mkdir -p "$HOME/.config/data-stack"
cp deploy/local.env.example "$HOME/.config/data-stack/local.env"
make doctor
make build
make import-images
make deploy
make status
```

`build` сохраняет ссылки на оба образа в `local.env`. Они переживают закрытие
терминала. `import-images` копирует образы из Docker в containerd k3s и требует sudo.
`deploy` создаёт namespace и при первом live-запуске запрашивает токен без эха.
Последующие запуски сохраняют пароли и токен. Никакого push в registry не требуется.
Для параметров Helm задайте `VALUES_FILE=/absolute/path/values.yaml` в `local.env`.
Локальные настройки и секреты не коммитьте.

## Demo без регистрации в API

На отдельной VM/кластере измените две строки `local.env`:

```bash
NAMESPACE=data-stack-demo
SOURCE_MODE=demo
```

Затем `make deploy`, `make demo`, `make status`. Выделяется отдельный DWH, Garage,
Kafka и Metabase. Получится 3 синтетических матча и 3 команды; повтор не добавит
дубликаты, обновление изменит счёт первого матча с 2:0 на 4:0. Сумма голов — 9.
На 8 GiB VM не запускайте одновременно live и demo полные стеки.
Demo никогда не включается автоматически при ошибке API.

## Windows: одно окно для интерфейсов

Скопируйте `scripts/open-ui.ps1` на Windows через `scp -F NUL`, сохранив его
как `$HOME\open-data-stack.ps1`.
Рядом с пользовательскими настройками создайте `%USERPROFILE%\.config\data-stack\ui.json`
по образцу `deploy/ui.example.json` и укажите свой IP, SSH-пользователя и путь проекта.
Запуск в PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$HOME\open-data-stack.ps1"
```

Без файла скрипт спросит IP и пользователя. Также доступны параметры `-VmAddress`,
`-RemoteUser`, `-ProjectPath`. Оставьте окно открытым. Старые туннели сначала
остановите Ctrl+C. Airflow: `http://localhost:28080`, Metabase: `http://localhost:23001`,
Prometheus: `http://localhost:29090`. На VM сервисы запускаются вместе с k3s;
туннель нужен только для браузера. Для demo добавьте `-Namespace data-stack-demo`
к команде PowerShell или задайте поле `namespace` в локальном JSON.

## Первая live-загрузка

Логин Airflow — `admin`. Пароль:

```bash
kubectl -n data-stack get secret stack-secrets -o jsonpath='{.data.AIRFLOW_AUTH_PASSWORDS}' | base64 -d
```

Включите `football_ingestion`, нажмите Trigger DAG. `raw_key` оставьте пустым.
Для обычного запуска даты тоже оставьте пустыми; для истории задайте обе даты
и при необходимости год начала сезона. За короткое окно матчей может не быть.
Задача `verify` объясняет результат; данные можно проверить через `make status`.
Просроченный доступ к историческим сезонам определяется тарифом API, не кодом проекта.

## Обновление существующего стенда 0.2

1. Сохраните backup: `make backup`, дождитесь завершения Job и сохраните дампы вне VM.
2. Не меняйте `sql/001_initial.sql`: версия 0.3 добавляет миграцию `002`.
3. `make build`, `make import-images`, `make deploy`.
4. `make status`, `bash scripts/smoke.sh`.
5. В Metabase выполните синхронизацию схем. Новые представления:
   `dm.team_season_stats`, `dm.ingestion_status`.

При первом добавлении защиты сессий Metabase может потребоваться повторный вход.
Все существующие пароли сохраняются; добавляются два новых постоянных ключа.
Сохраните обновлённый Secret отдельно в зашифрованной резервной копии.

При переходе с незашифрованной Metabase `deploy.sh` выполняет одноразовый
`enable-encryption`: останавливает Metabase, запускает Job с новым ключом из
Secret и возвращает приложение в работу. Bootstrap ставит метку pending только
при первом добавлении ключа в существующий Secret; после успешной операции
метка удаляется. Сначала обязательно сохраните backup.

Не добавляйте эту метку вручную для исправления неверного или утраченного ключа:
нужно восстановить прежний ключ/резервную копию. Повторные деплои не шифруют БД
заново. Основание: [процедура Metabase](https://www.metabase.com/docs/latest/databases/encrypting-details-at-rest).
