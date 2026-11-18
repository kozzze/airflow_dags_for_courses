import os
import boto3
import urllib3
from airflow.models import Variable
from clickhouse_driver import Client

urllib3.disable_warnings()

S3_BUCKET = "dad-ksu-datalab"
S3_ENDPOINT = "https://storage.yandexcloud.net"
MY_LOGIN = "stroinyi_aa"  
CH_HOST = "111.88.243.32" 

# --- НАСТРОЙКИ ДЛЯ CLICKHOUSE ---
S3_COLLECTION = "s3_dad_ksu_datalab"
S3_FILENAME = f"dad-ksu-datalab/DATA/{MY_LOGIN}/*.json"

STG_COLUMNS = (
    "event_id String, event_time String, receipt_id String, receipt_status String, "
    "store_city String, store_region String, product_id String, product_category String, "
    "product_brand String, customer_id String, customer_segment String, quantity Int32, "
    "gross_amount Float64, discount_amount Float64, net_amount Float64"
)

DIMS = [
    "dim_date",
    "dim_product",
    "dim_customer",
    "dim_region"
]

# Обновленный запрос: используем ИМЕНОВАННУЮ КОЛЛЕКЦИЮ вместо access_key и secret_key
LOAD_STG = f"""
insert into stroinyi_aa_db.stg_events
select *
from s3(
    {S3_COLLECTION},
    filename = '{S3_FILENAME}',
    format = 'JSONEachRow',
    structure = '{STG_COLUMNS}'
)
"""

DIM_SQL = {
    "dim_date": f"""
        insert into stroinyi_aa_db.dim_date_stage
        select 0, toDate('1970-01-01'), toUInt8(0), toUInt16(0)
        union all
        select
            toUInt32(formatDateTime(dt, '%Y%m%d')),
            toDate(dt),
            toUInt8(toMonth(dt)),
            toUInt16(toYear(dt))
        from (
            select distinct toDateTime(parseDateTimeBestEffort(event_time)) as dt
            from stroinyi_aa_db.stg_events
        )
        order by 1
    """,
    "dim_product": f"""
        insert into stroinyi_aa_db.dim_product_stage
        select product_id, any(product_category), any(product_brand)
        from stroinyi_aa_db.stg_events
        group by 1 order by 1
    """,
    "dim_customer": f"""
        insert into stroinyi_aa_db.dim_customer_stage
        select customer_id, any(customer_segment)
        from stroinyi_aa_db.stg_events
        group by 1 order by 1
    """,
    "dim_region": f"""
        insert into stroinyi_aa_db.dim_region_stage
        select concat(store_region, '_', store_city) as region_code, 
               any(store_city) as region_name, 'Россия' as country
        from stroinyi_aa_db.stg_events
        group by 1 order by 1
    """
}

FACT_SQL = f"""
insert into stroinyi_aa_db.fact_orders
select
    event_id                                    AS order_id,
    toDate(parseDateTimeBestEffort(event_time)) AS event_date,
    customer_id,
    receipt_status,
    store_city,
    store_region,
    product_id,
    product_category,
    product_brand,
    toUInt32(quantity)                          AS quantity,
    toDecimal64(gross_amount, 2)                AS gross_amount,
    toDecimal64(discount_amount, 2)             AS discount_amount,
    toDecimal64(net_amount, 2)                  AS net_amount
from (
    select *, toDateTime(parseDateTimeBestEffort(event_time)) as dt
    from stroinyi_aa_db.stg_events
    order by event_time
    limit 1 by event_id
)
"""

def check_duplicates(client, table_name, key_column):
    """Проверяет наличие дублей по ключу в таблице и выводит результат в логи."""
    query = f"""
        SELECT count() FROM (
            SELECT {key_column} 
            FROM stroinyi_aa_db.{table_name} 
            GROUP BY {key_column} 
            HAVING count() > 1
        )
    """
    dup_count = client.execute(query)[0][0]
    if dup_count > 0:
        print(f"⚠️ ВНИМАНИЕ: В таблице {table_name} найдено {dup_count} дубликатов по ключу {key_column}!")
    else:
        print(f"✅ В таблице {table_name} дубликатов нет.")

def rebuild_dimensions(client):
    # Автоматически создаем пустые таблицы, если их нет в базе!
    ddl_dims = {
        "dim_date": "date_key UInt32, full_date Date, month_num UInt8, year_num UInt16",
        "dim_product": "product_id String, category String, brand String",
        "dim_customer": "customer_id String, segment String",
        "dim_region": "region_code String, region_name String, country String"
    }
    
    # Словарь для проверки дублей (какой ключ проверять в какой таблице)
    key_map = {
        "dim_date": "date_key",
        "dim_product": "product_id",
        "dim_customer": "customer_id",
        "dim_region": "region_code"
    }
    
    for dim in DIMS:
        client.execute(f"create table if not exists stroinyi_aa_db.{dim} ({ddl_dims[dim]}) engine=MergeTree order by tuple()")
        
        stage = dim + "_stage"
        client.execute(f"create table if not exists stroinyi_aa_db.{stage} as stroinyi_aa_db.{dim}")
        client.execute(f"truncate table stroinyi_aa_db.{stage}")
        
        client.execute(DIM_SQL[dim])
        client.execute(f"exchange tables stroinyi_aa_db.{dim} and stroinyi_aa_db.{stage}")
        client.execute(f"truncate table stroinyi_aa_db.{stage}")
        
        rows = client.execute(f"select count() from stroinyi_aa_db.{dim}")[0][0]
        print(f"{dim}: {rows} строк")
        
        # ВЫПОЛНЯЕМ ПРОВЕРКУ НА ДУБЛИКАТЫ
        check_duplicates(client, dim, key_map[dim])

def load_s3_to_clickhouse(**context) -> None:
    ch_user = Variable.get("clickhouse_tech_user_account")
    ch_pass = Variable.get("clickhouse_tech_user_password")

    with Client(
        host=CH_HOST,
        user=ch_user,
        password=ch_pass,
        database=f"{MY_LOGIN}_db",
        port=9000,
    ) as client:
        # 1. Готовим стейджинг
        client.execute(f"create table if not exists stroinyi_aa_db.stg_events as stroinyi_aa_db.fact_orders_raw")
        client.execute("truncate table stroinyi_aa_db.stg_events")
        
        # 2. Грузим данные из S3 (силами ClickHouse с использованием коллекции)
        client.execute(LOAD_STG)

        total = client.execute("select count() from stroinyi_aa_db.stg_events")[0][0]
        print(f"в staging {total} строк")

        if total == 0:
            print("staging пуст, справочники и факт не трогаем")
            return

        # 3. Перестраиваем справочники (с проверкой на дубли)
        rebuild_dimensions(client)

        # 4. Заполняем факты
        client.execute("""
            create table if not exists stroinyi_aa_db.fact_orders (
                order_id String, event_date Date, customer_id String, receipt_status String,
                store_city String, store_region String, product_id String, product_category String,
                product_brand String, quantity UInt32, gross_amount Decimal(18, 2),
                discount_amount Decimal(18, 2), net_amount Decimal(18, 2)
            ) ENGINE = MergeTree ORDER BY (event_date, order_id)
        """)
        
        client.execute("truncate table stroinyi_aa_db.fact_orders")
        client.execute(FACT_SQL)

        fact_rows = client.execute("select count() from stroinyi_aa_db.fact_orders")[0][0]
        print(f"fact_orders: {fact_rows} строк")