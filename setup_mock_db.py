"""
setup_mock_db.py
Creates the mock e-commerce schema in your local MySQL database (demodb)
and inserts sample data so the app can generate recommendations.
"""
import mysql.connector
import yaml
import os
import random

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(BASE_DIR, 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)['database']

conn = mysql.connector.connect(
    host=config['host'],
    port=config['port'],
    user=config['user'],
    password=config['password'],
)
cursor = conn.cursor()

# Create database if not exists
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
cursor.execute(f"USE {config['database']}")
print(f"Using database: {config['database']}")

# ── Drop and recreate tables ────────────────────────────────────────────────
DDL = """
DROP TABLE IF EXISTS order_items, payments, reviews, sessions,
                     inventory, orders, products, categories, sellers, users;

CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(100),
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    created_at DATETIME DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'active',
    loyalty_points INT DEFAULT 0
);

CREATE TABLE categories (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    parent_id INT,
    slug VARCHAR(100)
);

CREATE TABLE sellers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(255),
    rating DECIMAL(3,2),
    city VARCHAR(100),
    joined_at DATETIME DEFAULT NOW()
);

CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    price DECIMAL(10,2),
    category_id INT,
    seller_id INT,
    stock INT DEFAULT 100,
    rating DECIMAL(3,2),
    created_at DATETIME DEFAULT NOW(),
    is_active TINYINT DEFAULT 1
);

CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    total_amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending',
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW(),
    shipping_address VARCHAR(255),
    payment_id INT
);

CREATE TABLE order_items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
);

CREATE TABLE reviews (
    id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT,
    user_id INT,
    rating DECIMAL(3,2),
    comment TEXT,
    created_at DATETIME DEFAULT NOW(),
    helpful_votes INT DEFAULT 0
);

CREATE TABLE sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    ip_address VARCHAR(45),
    device VARCHAR(50),
    started_at DATETIME DEFAULT NOW(),
    last_active DATETIME DEFAULT NOW(),
    is_active TINYINT DEFAULT 1
);

CREATE TABLE payments (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    method VARCHAR(50),
    amount DECIMAL(10,2),
    status VARCHAR(20),
    paid_at DATETIME DEFAULT NOW()
);

CREATE TABLE inventory (
    id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT,
    warehouse_id INT,
    quantity INT,
    updated_at DATETIME DEFAULT NOW()
);
"""

for stmt in DDL.strip().split(';'):
    stmt = stmt.strip()
    if stmt:
        cursor.execute(stmt)
conn.commit()
print("Tables created.")

# ── Insert sample data ──────────────────────────────────────────────────────
rng = random.Random(42)

cities = ['Mumbai', 'Delhi', 'Bengaluru', 'Chennai', 'Hyderabad']
statuses = ['pending', 'shipped', 'delivered', 'cancelled']
methods  = ['credit_card', 'debit_card', 'upi', 'net_banking', 'wallet']
devices  = ['mobile', 'desktop', 'tablet']

for i in range(50):
    cursor.execute("INSERT INTO categories (name, slug) VALUES (%s, %s)",
                   (f'Category {i}', f'cat-{i}'))
for i in range(50):
    cursor.execute("INSERT INTO sellers (name, email, rating, city) VALUES (%s,%s,%s,%s)",
                   (f'Seller {i}', f'seller{i}@shop.com', round(rng.uniform(3,5),2), rng.choice(cities)))

conn.commit()

for i in range(200):
    cursor.execute("INSERT INTO users (email,username,phone,city,country,status,loyalty_points) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                   (f'user{i}@example.com', f'user{i}', f'98{rng.randint(10000000,99999999)}',
                    rng.choice(cities), 'India', 'active', rng.randint(0,5000)))
for i in range(200):
    cursor.execute("INSERT INTO products (name,price,category_id,seller_id,stock,rating) VALUES (%s,%s,%s,%s,%s,%s)",
                   (f'Product {i}', round(rng.uniform(50,50000),2),
                    rng.randint(1,50), rng.randint(1,50),
                    rng.randint(0,500), round(rng.uniform(1,5),2)))

conn.commit()

for i in range(300):
    uid  = rng.randint(1,200)
    amt  = round(rng.uniform(100,25000),2)
    st   = rng.choice(statuses)
    yr   = rng.randint(2020,2024)
    mo   = rng.randint(1,12)
    dy   = rng.randint(1,28)
    cursor.execute(
        "INSERT INTO orders (user_id,total_amount,status,created_at,updated_at,shipping_address) VALUES (%s,%s,%s,%s,%s,%s)",
        (uid, amt, st, f'{yr}-{mo:02d}-{dy:02d} 10:00:00',
         f'{yr}-{mo:02d}-{dy:02d} 12:00:00', f'{rng.randint(1,99)} Main St'))
    cursor.execute("INSERT INTO payments (order_id,method,amount,status) VALUES (%s,%s,%s,%s)",
                   (i+1, rng.choice(methods), amt, 'completed'))
    cursor.execute("INSERT INTO inventory (product_id,warehouse_id,quantity) VALUES (%s,%s,%s)",
                   (rng.randint(1,200), rng.randint(1,5), rng.randint(0,1000)))
    cursor.execute("INSERT INTO sessions (user_id,ip_address,device) VALUES (%s,%s,%s)",
                   (uid, f'192.168.{rng.randint(1,254)}.{rng.randint(1,254)}', rng.choice(devices)))
    cursor.execute("INSERT INTO reviews (product_id,user_id,rating,comment) VALUES (%s,%s,%s,%s)",
                   (rng.randint(1,200), uid, round(rng.uniform(1,5),1), 'Great product!'))
    cursor.execute("INSERT INTO order_items (order_id,product_id,quantity,unit_price) VALUES (%s,%s,%s,%s)",
                   (i+1, rng.randint(1,200), rng.randint(1,5), round(rng.uniform(50,5000),2)))

conn.commit()
cursor.close()
conn.close()

print("Mock database ready! 200 users, 200 products, 300 orders (and more) loaded into 'demodb'.")
print("Now click 'ANALYZE FILE' in the dashboard to see recommendations.")
