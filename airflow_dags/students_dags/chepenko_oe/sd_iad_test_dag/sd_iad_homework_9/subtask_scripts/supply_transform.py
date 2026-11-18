"""Валидация и приведение событий поставок к единому набору колонок."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

REQUIRED = (
    "supply_line_id", "supply_id", "event_time", "supplier_id",
    "product_id", "warehouse_id", "ordered_qty", "received_qty",
    "received_amount", "delay_days",
)

COLUMNS = [
    "supply_line_id", "supply_id", "event_time", "supplier_id",
    "supplier_name", "supplier_country", "product_id", "product_name",
    "product_category", "product_brand", "warehouse_id", "warehouse_name",
    "warehouse_region", "ordered_qty", "received_qty", "shortage_qty",
    "received_amount", "unit_price", "delay_days", "event_type",
]


def parse_event_time(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("event_time должен быть непустой ISO-строкой")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).replace(tzinfo=None)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def normalize_supply_message(message):
    if not isinstance(message, dict):
        raise ValueError("Сообщение должно быть JSON-объектом")

    missing = [key for key in REQUIRED if message.get(key) is None]
    if missing:
        raise ValueError(f"Отсутствуют обязательные поля: {missing}")

    if message.get("event_type") != "supply_line":
        raise ValueError(f"Неожиданный event_type: {message.get('event_type')}")

    try:
        ordered = int(message["ordered_qty"])
        received = int(message["received_qty"])
        delay = int(message["delay_days"])
        amount = Decimal(str(message["received_amount"]))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError("Некорректный числовой формат в сообщении") from exc

    if min(ordered, received, delay) < 0 or amount < 0:
        raise ValueError("Количество, сумма и задержка не могут быть отрицательными")

    event_time = parse_event_time(message["event_time"])
    shortage = max(ordered - received, 0)
    unit_price = amount / Decimal(received) if received else Decimal("0")

    row = {
        "supply_line_id": str(message["supply_line_id"]),
        "supply_id": str(message["supply_id"]),
        "event_time": event_time,
        "supplier_id": str(message["supplier_id"]),
        "supplier_name": message.get("supplier_name"),
        "supplier_country": message.get("supplier_country"),
        "product_id": str(message["product_id"]),
        "product_name": message.get("product_name"),
        "product_category": message.get("product_category"),
        "product_brand": message.get("product_brand"),
        "warehouse_id": str(message["warehouse_id"]),
        "warehouse_name": message.get("warehouse_name"),
        "warehouse_region": message.get("warehouse_region"),
        "ordered_qty": ordered,
        "received_qty": received,
        "shortage_qty": shortage,
        "received_amount": float(amount),
        "unit_price": float(unit_price),
        "delay_days": delay,
        "event_type": "supply_line",
    }
    return row
