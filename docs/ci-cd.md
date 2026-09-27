# CI, публикация образов и необязательный CD

`ci.yml` выполняет lint, pytest, настоящие PostgreSQL/Garage/Kafka, запуск StatsD
с mapping из Helm, kubeconform, promtool, сборку образов, проверку Airflow API и DAG,
проверку зависимостей и Trivy. На main публикует проверенные образы в GHCR по SHA.
На PR образы не публикуются. Локальный запуск не зависит от GitHub Actions.

`windows.yml` проверяет разбор PowerShell в Windows PowerShell, используемом Windows 10.
`acceptance.yml` запускается вручную: одноразовый k3s через k3d, полный Helm chart,
demo, replay и проверка DWH. На VM с 8 GiB одновременно с live-стендом его не запускайте.
Минимум 6 GiB доступной памяти; временный кластер удаляется после проверки.

`deploy.yml` — отдельный workflow_dispatch, только main, окружение lab.
Развёртывает опубликованные для текущего SHA образы по digest. Нужны repository
variables DEPLOY_HOST/DEPLOY_USER и secrets DEPLOY_SSH_KEY/DEPLOY_KNOWN_HOSTS.
Настройте protection rules окружения lab при необходимости ручного одобрения.

Адрес вида 192.168.x.x недоступен GitHub-hosted runner без сетевого маршрута.
До настройки overlay/VPN используйте ручной локальный deploy. Не открывайте
Kubernetes API/БД в интернет ради CD. Постоянный self-hosted runner на VM с
приватными данными не должен исполнять произвольные PR публичного репозитория.

Actions закреплены SHA. Dependabot группирует еженедельные обновления Actions/Python;
Airflow, providers и constraints обновляйте одной проверенной PR. Образы runtime
в Helm и базовые Docker images закреплены версиями; обновления проходят acceptance.
Python приложения и CI — 3.12. requirements-dev использует тот же constraints-файл.

Проверки/бэйджи следует считать действующими только после успешного workflow
в вашем GitHub. Наличие YAML само по себе не является доказательством запуска.

## Первый push: порядок действий

1. Отправьте код вместе с `.github/workflows/` в `main`. Эта ветка должна быть
   основной веткой репозитория. В Settings → Actions → General разрешите Actions
   и используемые сторонние actions, если они отключены политикой репозитория.
2. Откройте **Actions → Test and publish images**. Push в main запускает CI
   автоматически: сначала `checks`, затем сборку, проверки и публикацию двух образов.
   В pull request проходят проверки и сборка, но публикации образов нет.
   Обычный push в другую ветку без PR этот workflow не запускает.
3. Для CI не нужны рабочий football API token, kubeconfig или SSH-ключ VM.
   Тестовые сервисы создаются на runner. Для GHCR используется автоматический
   `GITHUB_TOKEN`; право `packages: write` уже задано в job публикации.
4. Успешные образы появятся в Packages: `data-stack-airflow` и
   `data-stack-consumer`, тег — полный SHA коммита. Проверьте доступность пакетов:
   для анонимного pull из k3s они должны быть public; для private пакетов нужен
   registry Secret с правом чтения пакетов и `imagePullSecrets` в Helm values.
5. После успешного CI вручную запустите **Actions → Full k3s acceptance (manual)
   → Run workflow → main**. Windows launcher проверяется через отдельный
   **Check Windows launcher → Run workflow**, а также при PR с изменением launcher.
6. **Optional manual deployment** пока не запускайте: обычный hosted runner не
   видит локальную VM `192.168.x.x`. Первый push и CI сами по себе её не обновляют.
   До настройки сетевого соединения продолжайте локальное развёртывание.

Когда будет настроен сетевой доступ для CD:

- Settings → Environments → создайте `lab`.
- Settings → Secrets and variables → Actions → Variables:
  `DEPLOY_HOST` (доступный runner адрес) и `DEPLOY_USER`.
- В Secrets добавьте `DEPLOY_SSH_KEY` (отдельный приватный ключ для deploy)
  и `DEPLOY_KNOWN_HOSTS` (проверенная запись SSH host key сервера).
- На VM должны работать k3s, kubectl, Helm и Python; у deploy-пользователя —
  доступ к кластеру через kubeconfig. У кластера должен быть доступ к образам GHCR.
- Дождитесь успешного CI для **текущего коммита main**, затем Actions →
  Optional manual deployment → Run workflow → main. CD ожидает уже опубликованные
  образы этого SHA; ожидание завершения CI автоматически в нём не реализовано.

Кнопка Run workflow появляется после попадания workflow с `workflow_dispatch`
в основную ветку. Если проверка упала, откройте красный job и первый упавший шаг:
исправьте причину, сделайте новый коммит и push. Красный CI не отменяет уже
сделанный Git-коммит и не изменяет локальный стенд.

Справочники GitHub: [ручной запуск](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow),
[публикация образов](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images),
[доступ к частной сети](https://docs.github.com/en/actions/how-tos/manage-runners/github-hosted-runners/connect-to-a-private-network).
