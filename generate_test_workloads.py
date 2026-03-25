import random
import os

rng = random.Random(42)
def rand_id(): return rng.randint(1, 100000)

def generate_basic_workload():
    qs = []
    # Good
    for _ in range(50):
        qs.append(f"SELECT id, email FROM users WHERE id = {rand_id()}")
        qs.append(f"SELECT * FROM products WHERE price > 50 AND is_active = 1")
        qs.append(f"INSERT INTO users (email, username) VALUES ('test{rand_id()}@example.com', 'user{rand_id()}')")
        qs.append(f"UPDATE products SET stock = stock - 1 WHERE id = {rand_id()} AND stock > 0")

    # Anti-patterns
    for _ in range(20):
        qs.append(f"SELECT * FROM users")
        qs.append(f"SELECT * FROM products WHERE name LIKE '%laptop'")
        qs.append(f"DELETE FROM sessions")
        qs.append(f"SELECT u.id, p.name FROM users u, products p")
        qs.append(f"SELECT * FROM users ORDER BY RAND()")

    rng.shuffle(qs)
    return qs

def generate_enterprise_workload():
    qs = []
    # Trigger PARTITION (Heavy filtering on created_at or updated_at, timestamp/date column > 20 times)
    for _ in range(50):
        qs.append(f"SELECT * FROM orders WHERE created_at > '2023-01-01' AND created_at < '2024-01-01'")
        qs.append(f"SELECT total_amount FROM orders WHERE created_at BETWEEN '2020-01-01' AND '2022-01-01'")
        qs.append(f"SELECT * FROM inventory WHERE updated_at < NOW()")

    # Enterprise features (CTEs, Window Functions) - to test sqlglot parser
    for _ in range(10):
        # CTE
        qs.append(f"WITH top_users AS (SELECT user_id, SUM(total_amount) as spent FROM orders GROUP BY user_id ORDER BY spent DESC LIMIT 10) SELECT u.email, t.spent FROM users u JOIN top_users t ON u.id = t.user_id")
        # Window Function
        qs.append(f"SELECT user_id, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created_at DESC) as order_rank FROM orders")

    # Anti-patterns (Not In Subquery, Function on Column)
    for _ in range(20):
        qs.append(f"SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)")
        qs.append(f"SELECT * FROM users WHERE YEAR(created_at) = 2023")

    rng.shuffle(qs)
    return qs

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    basic = generate_basic_workload()
    basic_path = os.path.join(base_dir, 'test_workload_basic.txt')
    with open(basic_path, 'w', encoding='utf-8') as f:
        for q in basic: f.write(q + ';\n')
        
    enterprise = generate_enterprise_workload()
    ent_path = os.path.join(base_dir, 'test_workload_enterprise.txt')
    with open(ent_path, 'w', encoding='utf-8') as f:
        for q in enterprise: f.write(q + ';\n')

    print("Successfully generated test_workload_basic.txt and test_workload_enterprise.txt")
