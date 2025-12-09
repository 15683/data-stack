# data-stack
Simple educational data engineering project with Airflow, PostgreSQL, DuckDB, MinIO, Metabase

Вся инфраструктура развёрнута в Docker на VPS ruvds.com (2x2.2ГГц, 7Гб RAM, 70Гб SSD RAID)

Сырые данные берутся из API https://www.football-data.org/ (https://native-stats.org/competition/CL/)

![flow](https://github.com/15683/data-stack/raw/main/images/flow.png)

![flow](https://github.com/15683/data-stack/raw/main/images/dag.png)

![flow](https://github.com/15683/data-stack/raw/main/images/dag_dep.png)

![flow](https://github.com/15683/data-stack/raw/main/images/minio.png)

![flow](https://github.com/15683/data-stack/raw/main/images/bi.png)