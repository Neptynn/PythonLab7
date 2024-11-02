from faker import Faker
import psycopg2
import random
from datetime import timedelta, date

fake = Faker('uk_UA')

try:
    # Підключення до бази даних PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port="5433",
        database="ShopDB",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    # Створення таблиць
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Clients (
        client_id SERIAL PRIMARY KEY,
        client_name VARCHAR(100) NOT NULL,
        entity_type VARCHAR(10) CHECK (entity_type IN ('юридична', 'фізична')),
        address TEXT,
        phone VARCHAR(20),
        contact_person VARCHAR(50),
        account_number VARCHAR(120) UNIQUE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        stock INT DEFAULT 0
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Sales (
        sale_id SERIAL PRIMARY KEY,
        sale_date DATE NOT NULL,
        client_id INT REFERENCES Clients(client_id),
        product_id INT REFERENCES Products(product_id),
        quantity INT NOT NULL CHECK (quantity > 0),
        discount DECIMAL(3, 2) CHECK (discount BETWEEN 0.03 AND 0.20),
        payment_form VARCHAR(15) CHECK (payment_form IN ('готівковий', 'безготівковий')),
        delivery_needed BOOLEAN DEFAULT FALSE,
        delivery_cost DECIMAL(10, 2) DEFAULT 0
    );
    """)

    # Генерація та додавання клієнтів
    clients_data = []
    for _ in range(4):
        client_name = fake.company()
        entity_type = random.choice(['юридична', 'фізична'])
        address = fake.address()
        phone = fake.numerify(text='+38(0##)-##-##-###')
        contact_person = fake.name()
        account_number = fake.unique.numerify(text='#############')
        clients_data.append((client_name, entity_type, address, phone, contact_person, account_number))

    cur.executemany("""
    INSERT INTO Clients (client_name, entity_type, address, phone, contact_person, account_number)
    VALUES (%s, %s, %s, %s, %s, %s);
    """, clients_data)

    # Генерація та додавання товарів
    products_data = []
    for _ in range(10):
        product_name = fake.word()
        price = round(random.uniform(5, 100), 2)
        stock = random.randint(50, 500)
        products_data.append((product_name, price, stock))

    cur.executemany("""
    INSERT INTO Products (product_name, price, stock)
    VALUES (%s, %s, %s);
    """, products_data)

    # Генерація та додавання продажів
    sales_data = []
    start_date = date(2024, 10, 1)
    for _ in range(19):
        sale_date = start_date + timedelta(days=random.randint(0, 30))
        client_id = random.randint(1, len(clients_data))
        product_id = random.randint(1, len(products_data))
        quantity = random.randint(1, 20)
        discount = round(random.uniform(0.03, 0.20), 2)
        payment_form = random.choice(['готівковий', 'безготівковий'])
        delivery_needed = random.choice([True, False])
        delivery_cost = round(random.uniform(0, 150), 2) if delivery_needed else 0
        sales_data.append((sale_date, client_id, product_id, quantity, discount, payment_form, delivery_needed, delivery_cost))

    cur.executemany("""
    INSERT INTO Sales (sale_date, client_id, product_id, quantity, discount, payment_form, delivery_needed, delivery_cost)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, sales_data)

    conn.commit()
    cur.close()
    conn.close()
    print("Дані успішно згенеровано та додано до таблиць!")

except Exception as e:
    print(f"Помилка підключення: {e}")