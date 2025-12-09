# Учебный проект Football Data Pipeline

Автоматический сбор, хранение и анализ статистики Лиги Чемпионов.

Вся инфраструктура развёрнута в Docker на VPS ruvds.com (2x2.2ГГц, 7Гб RAM, 70Гб SSD RAID)

Сырые данные берутся из API https://www.football-data.org/ (https://native-stats.org/competition/CL/)

![flow](https://github.com/15683/data-stack/raw/main/images/flow.png)

![dag](https://github.com/15683/data-stack/raw/main/images/dag.png)

![dag_dep](https://github.com/15683/data-stack/raw/main/images/dag_dep.png)

![minio](https://github.com/15683/data-stack/raw/main/images/minio.png)

![bi](https://github.com/15683/data-stack/raw/main/images/bi.png)