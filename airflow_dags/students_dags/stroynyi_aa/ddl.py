import requests
import pandas as pd
from io import StringIO

CH_HOST = "111.88.243.32"
CH_PORT = 8123
CH_USER = "stroinyi_aa"
CH_PASSWORD = "EWfHrx7wucqd" # <-- ВАШ ПАРОЛЬ

url = f"http://{CH_HOST}:{CH_PORT}/"
auth = (CH_USER, CH_PASSWORD)

def run_query(sql):
    response = requests.post(url, params={"query": sql + " FORMAT CSV"}, auth=auth)
    if response.status_code == 200:
        return pd.read_csv(StringIO(response.text))
    else:
        print(f"❌ Ошибка: {response.text}")
        return None

print("--- 1. Проверка количества строк ---")
tables = ["fact_orders", "dim_date", "dim_product", "dim_customer", "dim_region"]
for table in tables:
    df = run_query(f"SELECT count() as cnt FROM stroinyi_aa_db.{table}")
    if df is not None and not df.empty:
        print(f"✅ {table}: {df.iloc[0]['cnt']} строк")

print("\n--- 2. Пример данных из витрины (fact_orders) ---")
df_fact = run_query("SELECT * FROM stroinyi_aa_db.fact_orders LIMIT 5")
if df_fact is not None:
    print(df_fact.to_string())

print("\n--- 3. Проверка связей (JOIN фактов и справочников) ---")
# Проверяем, что ID в фактах совпадают с ID в справочниках
sql_join = """
    SELECT 
        f.order_id, 
        f.event_date, 
        p.category as product_category, 
        c.segment as customer_segment, 
        r.region_name
    FROM stroinyi_aa_db.fact_orders f
    LEFT JOIN stroinyi_aa_db.dim_product p ON f.product_id = p.product_id
    LEFT JOIN stroinyi_aa_db.dim_customer c ON f.customer_id = c.customer_id
    LEFT JOIN stroinyi_aa_db.dim_region r ON concat(f.store_region, '_', f.store_city) = r.region_code
    LIMIT 5
"""
df_join = run_query(sql_join)
if df_join is not None:
    print(df_join.to_string())