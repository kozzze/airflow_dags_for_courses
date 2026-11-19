# sd_iad_homework_9

Учебный ETL-процесс для событий поставок:

`RabbitMQ (supply.queue) -> Yandex Object Storage (Parquet) -> ClickHouse`

## Структура

- `sd_iad_homework_9.py` — Airflow DAG и порядок выполнения задач.
- `subtask_scripts/config.py` — чтение Airflow Variables.
- `subtask_scripts/supply_transform.py` — валидация и нормализация события.
- `subtask_scripts/rabbit_to_s3.py` — пакетное получение сообщений и запись Parquet в S3.
- `subtask_scripts/s3_to_clickhouse.py` — чтение Parquet и загрузка в ClickHouse.

## Airflow Variables

Ожидаются переменные (секреты вводятся только в Airflow, не в GitHub):

| Назначение | Названия, которые проверяет код |
|---|---|
| RabbitMQ host | `rabbit_mq_host` или `rabbit_host` |
| RabbitMQ user | `rabbit_mq_svc_login` или `rabbit_user` |
| RabbitMQ password | `rabbit_mq_svc_password` или `rabbit_password` |
| S3 access key | `s3_dad_ksu_dataplatform_access_key`, `s3_dad_ksu_datalab_access_key` или `s3_access_key` |
| S3 secret key | `s3_dad_ksu_dataplatform_secret_key`, `s3_dad_ksu_datalab_secret_key` или `s3_secret_key` |
| ClickHouse host | `clickhouse_host` |
| ClickHouse user | `clickhouse_tech_user_account` или `clickhouse_user` |
| ClickHouse password | `clickhouse_tech_user_password` или `clickhouse_password` |
| ClickHouse port | `clickhouse_port` (по умолчанию 8123, HTTP) |
| RabbitMQ port/vhost/queue | `rabbit_mq_port`, `rabbit_mq_vhost`, `rabbit_mq_queue` (необязательные; defaults 5672, `/`, `supply.queue`) |
| Batch size | `supply_batch_size` (необязательная; default 500) |

## Важные детали

- S3 bucket: `dad-ksu-datalab`; prefix: `DATA/chepenko_oe/`.
- Формат промежуточного файла: Parquet со сжатием Snappy.
- RabbitMQ сообщения подтверждаются только после успешной записи Parquet в S3.
- При ошибке записи в S3 полученные сообщения возвращаются в очередь.
- В ClickHouse создаётся отдельная raw/staging-таблица `chepenko_oe_db.supply_lines_raw`. Исходные таблицы витрины не изменяются.
- Повторные загрузки могут физически создавать версии строк в ReplacingMergeTree; для дедупликации при чтении используй `FINAL` либо `argMax` по версии, если появится версия загрузки.

## Зависимости среды Airflow

На worker должны быть установлены:
`pika`, `boto3`, `pyarrow`, `clickhouse-connect`.

Проверить их наличие можно запуском DAG. Если пакет отсутствует, его должен установить администратор учебного Airflow/образа worker; установка из DAG не выполняется.

## Ограничения, которые нужно проверить в учебной среде

1. Наличие и точные имена Airflow Variables.
2. Доступность RabbitMQ, S3 и ClickHouse из Airflow worker.
3. ClickHouse HTTP port (в DBeaver ранее использовался 8123).
4. Права пользователя ClickHouse на создание БД/таблицы и вставку.
5. Совпадение структуры сообщения с ожидаемыми полями в `supply_transform.py`.

Запускать DAG следует вручную. При пустой очереди первая задача возвращает `None`, вторая корректно пропускает загрузку.
