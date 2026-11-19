"""Чтение настроек из Airflow Variables без хранения секретов в репозитории."""
from airflow.models import Variable


def get_var(names, default=None, required=True):
    if isinstance(names, str):
        names = [names]
    for name in names:
        value = Variable.get(name, default_var=None)
        if value not in (None, ""):
            return value
    if default is not None:
        return default
    if required:
        raise RuntimeError(
            "Не найдена Airflow Variable. Проверены названия: " + ", ".join(names)
        )
    return None


def get_connection_settings():
    return {
        "rabbit_host": get_var(["rabbit_mq_host", "rabbit_host"]),
        "rabbit_user": get_var(["rabbit_mq_svc_login", "rabbit_user"]),
        "rabbit_password": get_var(["rabbit_mq_svc_password", "rabbit_password"]),
        "rabbit_port": int(get_var(["rabbit_mq_port", "rabbit_port"], default="5672", required=False)),
        "rabbit_vhost": get_var(["rabbit_mq_vhost", "rabbit_vhost"], default="/", required=False),
        "s3_access": get_var(["s3_dad_ksu_dataplatform_access_key", "s3_dad_ksu_datalab_access_key", "s3_access_key"]),
        "s3_secret": get_var(["s3_dad_ksu_dataplatform_secret_key", "s3_dad_ksu_datalab_secret_key", "s3_secret_key"]),
        "ch_host": get_var(["clickhouse_host"]),
        "ch_user": get_var(["clickhouse_tech_user_account", "clickhouse_user"]),
        "ch_password": get_var(["clickhouse_tech_user_password", "clickhouse_password"]),
        "ch_port": int(get_var(["clickhouse_port"], default="8123", required=False)),
    }
