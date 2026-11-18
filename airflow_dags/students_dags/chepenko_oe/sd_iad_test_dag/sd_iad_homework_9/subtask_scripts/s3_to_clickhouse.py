"""Читает Parquet из Yandex Object Storage и загружает его в ClickHouse."""
import io
import logging

import boto3
import clickhouse_connect
import pyarrow.parquet as pq

from subtask_scripts.config import get_connection_settings

LOG = logging.getLogger(__name__)
BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
DATABASE = "chepenko_oe_db"
TABLE = "supply_lines_raw"


def load_s3_to_clickhouse(s3_path, **kwargs):
    if not s3_path:
        LOG.info("RabbitMQ не вернул новых сообщений; загрузка в ClickHouse пропущена.")
        return "no_data"

    cfg = get_connection_settings()
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=cfg["s3_access"],
        aws_secret_access_key=cfg["s3_secret"],
    )

    response = s3.get_object(Bucket=BUCKET, Key=s3_path)
    parquet_bytes = response["Body"].read()
    table = pq.read_table(io.BytesIO(parquet_bytes))
    rows = table.to_pylist()
    if not rows:
        LOG.info("Parquet-файл пуст: %s", s3_path)
        return "empty_file"

    # 8123 — HTTP(S) интерфейс ClickHouse; clickhouse-connect использует HTTP.
    client = clickhouse_connect.get_client(
        host=cfg["ch_host"],
        port=cfg["ch_port"],
        username=cfg["ch_user"],
        password=cfg["ch_password"],
        database=DATABASE,
        secure=False,
        connect_timeout=15,
        send_receive_timeout=60,
    )
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE}
            (
                supply_line_id String,
                supply_id String,
                event_time DateTime64(6),
                supplier_id String,
                supplier_name Nullable(String),
                supplier_country Nullable(String),
                product_id String,
                product_name Nullable(String),
                product_category Nullable(String),
                product_brand Nullable(String),
                warehouse_id String,
                warehouse_name Nullable(String),
                warehouse_region Nullable(String),
                ordered_qty Int64,
                received_qty Int64,
                shortage_qty Int64,
                received_amount Float64,
                unit_price Float64,
                delay_days Int64,
                event_type LowCardinality(String)
            )
            ENGINE = ReplacingMergeTree
            ORDER BY supply_line_id
        """)
        column_names = table.column_names
        insert_rows = [[row[column] for column in column_names] for row in rows]
        client.insert(
            f"{DATABASE}.{TABLE}",
            insert_rows,
            column_names=column_names,
        )
        LOG.info("В ClickHouse загружено %s строк из %s", len(rows), s3_path)
        return {"s3_path": s3_path, "rows_loaded": len(rows), "table": f"{DATABASE}.{TABLE}"}
    finally:
        client.close()
