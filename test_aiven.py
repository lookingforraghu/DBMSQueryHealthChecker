import mysql.connector
import sys

def test_aiven():
    host = "mysql-26ba13e5-raghun-7ad2.a.aivencloud.com"
    port = 28627
    user = "avnadmin"
    password = sys.argv[1]

    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            database="defaultdb"
        )
        print("Successfully connected!")
        conn.close()
    except Exception as e:
        print(f"Error connecting: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_aiven()
    else:
        print("No password provided.")
