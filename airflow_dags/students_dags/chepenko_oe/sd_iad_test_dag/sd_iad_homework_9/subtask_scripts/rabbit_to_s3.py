"""Забирает ограниченную пачку supply-событий и сохраняет Parquet в S3.

Сообщения подтверждаются только после успешной загрузки файла в S3.
При ошибке загрузки неподтверждённые сообщения возвращаются в очередь.
"""
import io
import json
import logging
import uuid
from datetime import datetime, timezone

import boto3
import pika
import pyarrow as pa
import pyarrow.parquet as pq

from subtask_scripts.config import get_connection_settings, get_var
from subtask_scripts.supply_transform import COLUMNS, normalize_supply_message

LOG = logging.getLogger(__name__)
BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
QUEUE = "supply.queue"
BATCH_SIZE = 1000


def fetch_rabbit_to_s3(login="chepenko_oe", **kwargs):
    cfg = get_connection_settings()
    queue = get_var(["rabbit_mq_queue", "rabbit_queue"], default=QUEUE, required=False)
    batch_size = int(get_var(["supply_batch_size"], default=str(BATCH_SIZE), required=False))

    credentials = pika.PlainCredentials(cfg["rabbit_user"], cfg["rabbit_password"])
    params = pika.ConnectionParameters(
        host=cfg["rabbit_host"],
        port=cfg["rabbit_port"],
        virtual_host=cfg["rabbit_vhost"],
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
        connection_attempts=3,
        retry_delay=5,
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    deliveries = []
    rows = []
    try:
        # basic_get не запускает бесконечный consumer: один DAG-run обрабатывает не более batch_size.
        for _ in range(batch_size):
            method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
            if method is None:
                break
            deliveries.append(method.delivery_tag)
            try:
                payload = json.loads(body.decode("utf-8"))
                rows.append(normalize_supply_message(payload))
            except Exception:
                # Невалидное сообщение не теряем: вся пачка будет возвращена в очередь.
                raise

        if not rows:
            LOG.info("Очередь %s пуста: выгружать нечего", queue)
            return None

        table = pa.Table.from_pylist(rows).select(COLUMNS)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        session = boto3.session.Session()
        s3 = session.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=cfg["s3_access"],
            aws_secret_access_key=cfg["s3_secret"],
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        object_key = f"DATA/{login}/supply_data_{timestamp}_{uuid.uuid4().hex[:8]}.parquet"
        s3.put_object(
            Bucket=BUCKET,
            Key=object_key,
            Body=buffer.getvalue(),
            ContentType="application/vnd.apache.parquet",
        )

        # ACK только после успешного put_object.
        for tag in deliveries:
            channel.basic_ack(delivery_tag=tag)

        LOG.info("В S3 записано %s строк: s3://%s/%s", len(rows), BUCKET, object_key)
        return object_key

    except Exception:
        LOG.exception("Ошибка RabbitMQ -> S3. Возвращаем полученные сообщения в очередь.")
        for tag in deliveries:
            try:
                channel.basic_nack(delivery_tag=tag, requeue=True)
            except Exception:
                LOG.exception("Не удалось выполнить NACK для delivery_tag=%s", tag)
        raise
    finally:
        if connection.is_open:
            connection.close()
