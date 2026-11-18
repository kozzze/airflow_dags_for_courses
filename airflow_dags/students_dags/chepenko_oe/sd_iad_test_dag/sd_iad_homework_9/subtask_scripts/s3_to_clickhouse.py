
"""Читает Parquet из Yandex Object Storage и загружает его в ClickHouse."""

import io
import json
import logging
from urllib.request import Request, urlopen
from urllib.parse import quote

import boto3
import pyarrow.parquet as pq

from subtask_scripts.config import get_connection_settings

LOG = logging.getLogger(__name__)

BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
DATABASE = "chepenko_oe_db"
TABLE = "supply_lines_raw"

COLUMNS = [
    "supply_line_id",
    "supply_id",
    "event_time",
    "supplier_id",
    "product_id",
    "warehouse_id",
    "ordered_qty",
    "received_qty",
    "received_amount",
    "delay_days",
    "shortage_qty",
    "unit_price",
]


def clickhouse_query(cfg, query):
    host = cfg["ch_host"].rstrip("/")
    if not host.startswith(("http://", "https://")):
        host = "http://" + host

    port = int(cfg["ch_port"])
    if ":" not in host.split("://", 1)[1].split("/", 1)[0]:
        host = f"{host}:{port}"

    url = (
        f"{host}/?database={quote(DATABASE)}"
        f"&default_format=TabSeparated"
    )

    credentials = (
        f'{cfg["ch_user"]}:{cfg["ch_password"]}'
    ).encode("utf-8")

    import base64
    auth = base64.b64encode(credentials).decode("ascii")

    request = Request(
        url,
        data=query.encode("utf-8"),
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "text/plain; charset=utf-8",
        },
        method="POST",
    )

    with urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8")


def load_s3_to_clickhouse(s3_path, **kwargs):
    if not s3_path or s3_path == "None":
        LOG.info("Нет новых сообщений для загрузки.")
        return "no_data"

    cfg = get_connection_settings()

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=cfg["s3_access"],
        aws_secret_access_key=cfg["s3_secret"],
    )

    response = s3.get_object(
        Bucket=BUCKET,
        Key=s3_path,
    )

    parquet_bytes = response["Body"].read()
    table = pq.read_table(io.BytesIO(parquet_bytes))

    missing = set(COLUMNS) - set(table.column_names)
    if missing:
        raise ValueError(
            f"В Parquet отсутствуют обязательные поля: {sorted(missing)}"
        )

    rows = table.select(COLUMNS).to_pylist()

    if not rows:
        LOG.info("Parquet-файл пуст: %s", s3_path)
        return "empty_file"

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE}
    (
        supply_line_id String,
        supply_id String,
        event_time DateTime64(6, 'UTC'),
        supplier_id String,
        product_id String,
        warehouse_id String,
        ordered_qty Float64,
        received_qty Float64,
        received_amount Float64,
        delay_days Int32,
        shortage_qty Float64,
        unit_price Float64
    )
    ENGINE = MergeTree()
    ORDER BY supply_line_id
    """

    clickhouse_query(cfg, create_table_sql)

    values = []
    for row in rows:
        values.append([
            row["supply_line_id"],
            row["supply_id"],
            row["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f"),
            row["supplier_id"],
            row["product_id"],
            row["warehouse_id"],
            row["ordered_qty"],
            row["received_qty"],
            row["received_amount"],
            row["delay_days"],
            row["shortage_qty"],
            row["unit_price"],
        ])

    insert_sql = (
        f"INSERT INTO {DATABASE}.{TABLE} "
        f"({', '.join(COLUMNS)}) FORMAT JSONEachRow\n"
    )

    payload = "".join(
        json.dumps(
            dict(zip(COLUMNS, row)),
            ensure_ascii=False,
        ) + "\n"
        for row in values
    )

    clickhouse_query(cfg, insert_sql + payload)

    LOG.info(
        "В ClickHouse загружено %s строк из %s",
        len(rows),
        s3_path,
    )

    return {
        "s3_path": s3_path,
        "rows_loaded": len(rows),
        "table": f"{DATABASE}.{TABLE}",
    }

