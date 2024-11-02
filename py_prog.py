import psycopg2
from tabulate import tabulate

# Параметри підключення до БД
conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="ShopDB",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# Функція для виводу структури та даних таблиці
def display_table_structure_and_data(table_name):
    # Структура таблиці
    print(f"\n--- Структура таблиці {table_name} ---")
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name.lower()}'")
    structure = cur.fetchall()
    print(tabulate(structure, headers=["Назва колонки", "Тип даних"], tablefmt="psql"))

    # Дані таблиці
    print(f"\n--- Дані таблиці {table_name} ---")
    cur.execute(f"SELECT * FROM {table_name}")
    data = cur.fetchall()
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name.lower()}'")
    headers = [row[0] for row in cur.fetchall()]
    print(tabulate(data, headers=headers, tablefmt="psql"))

# Виведення структури та даних для кожної таблиці
tables = ["Clients", "Products", "Sales"]
for table in tables:
    display_table_structure_and_data(table)

# Виконання та вивід запитів
queries = {
    "Продажі з оплатою готівкою, відсортовані за назвою клієнта": """
        SELECT Sales.*, Clients.client_name
        FROM Sales
        JOIN Clients ON Sales.client_id = Clients.client_id
        WHERE Sales.payment_form = 'готівковий'
        ORDER BY Clients.client_name;
    """,
    "Продажі з необхідністю доставки": """
        SELECT * FROM Sales WHERE delivery_needed = TRUE;
    """,
    "Сума та сума зі знижкою для кожного клієнта": """
        SELECT 
            Clients.client_id,
            Clients.client_name,
            SUM(Sales.quantity * Products.price) AS total_amount,
            SUM((Sales.quantity * Products.price) * (1 - Sales.discount)) AS discounted_amount
        FROM Sales
        JOIN Clients ON Sales.client_id = Clients.client_id
        JOIN Products ON Sales.product_id = Products.product_id
        GROUP BY Clients.client_id, Clients.client_name;
    """,
    "Кількість покупок для кожного клієнта": """
        SELECT 
            Clients.client_id,
            Clients.client_name,
            COUNT(Sales.sale_id) AS purchase_count
        FROM Sales
        JOIN Clients ON Sales.client_id = Clients.client_id
        GROUP BY Clients.client_id, Clients.client_name;
    """,
    "Сума, сплачена готівкою та безготівково кожним клієнтом": """
        SELECT 
            Clients.client_id,
            Clients.client_name,
            SUM(CASE WHEN Sales.payment_form = 'готівковий' THEN Sales.quantity * Products.price * (1 - Sales.discount) ELSE 0 END) AS cash_total,
            SUM(CASE WHEN Sales.payment_form = 'безготівковий' THEN Sales.quantity * Products.price * (1 - Sales.discount) ELSE 0 END) AS non_cash_total
        FROM Sales
        JOIN Clients ON Sales.client_id = Clients.client_id
        JOIN Products ON Sales.product_id = Products.product_id
        GROUP BY Clients.client_id, Clients.client_name;
    """
}

for description, query in queries.items():
    print(f"\n--- {description} ---")
    cur.execute(query)
    result = cur.fetchall()
    print(tabulate(result, tablefmt="psql"))

cur.close()
conn.close()