import requests

CH_HOST = "111.88.243.32"
CH_PORT = 8123
CH_USER = "stroinyi_aa"
CH_PASSWORD = "EWfHrx7wucqd" # <-- ВАШ ПАРОЛЬ

queries = [
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_customer",
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_customer_stage",
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_region",
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_region_stage",
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_product",
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_product_stage",
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_date",
    "DROP TABLE IF EXISTS stroinyi_aa_db.dim_date_stage",
    "DROP TABLE IF EXISTS stroinyi_aa_db.fact_orders",
    "DROP TABLE IF EXISTS stroinyi_aa_db.stg_events"
]

url = f"http://{CH_HOST}:{CH_PORT}/"

for q in queries:
    response = requests.post(url, params={"query": q}, auth=(CH_USER, CH_PASSWORD))
    if response.status_code != 200:
        print(f"❌ Ошибка: {response.text}")
        break
else:
    print("✅ Все старые таблицы удалены! Теперь код создаст их заново с правильной структурой.")