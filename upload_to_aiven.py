import os
import sys
import mysql.connector

# Replace these with your exact Aiven credentials
HOST = "mysql-26ba13e5-raghun-7ad2.a.aivencloud.com"
PORT = 28627
USER = "avnadmin"
DATABASE = "defaultdb"

def execute_sql_file(cursor, filepath):
    print(f"Executing: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        # A simple parser for large dump files (reads statement by statement)
        statement = ""
        for line in f:
            if line.strip().startswith('--') or not line.strip():
                continue
            statement += line
            if statement.strip().endswith(';'):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    print(f"Error on executing statement: {e}")
                statement = ""

def main():
    if len(sys.argv) > 1:
        password = sys.argv[1].strip()
    else:
        password = input("Enter your Aiven Password: ").strip()
    
    try:
        print("Connecting to Aiven...")
        conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=password,
            database=DATABASE,
            ssl_disabled=False # Aiven requires SSL
        )
        cursor = conn.cursor()
        print("Connected successfully!")
        
        # 1. Create schema directly in defaultdb
        schema_sql = """
        DROP TABLE IF EXISTS dept_emp, dept_manager, titles, salaries, employees, departments;
        
        CREATE TABLE employees (
            emp_no      INT             NOT NULL,
            birth_date  DATE            NOT NULL,
            first_name  VARCHAR(14)     NOT NULL,
            last_name   VARCHAR(16)     NOT NULL,
            gender      ENUM ('M','F')  NOT NULL,    
            hire_date   DATE            NOT NULL,
            PRIMARY KEY (emp_no)
        );

        CREATE TABLE departments (
            dept_no     CHAR(4)         NOT NULL,
            dept_name   VARCHAR(40)     NOT NULL,
            PRIMARY KEY (dept_no),
            UNIQUE  KEY (dept_name)
        );

        CREATE TABLE dept_manager (
           emp_no       INT             NOT NULL,
           dept_no      CHAR(4)         NOT NULL,
           from_date    DATE            NOT NULL,
           to_date      DATE            NOT NULL,
           FOREIGN KEY (emp_no)  REFERENCES employees (emp_no)    ON DELETE CASCADE,
           FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
           PRIMARY KEY (emp_no,dept_no)
        ); 

        CREATE TABLE dept_emp (
            emp_no      INT             NOT NULL,
            dept_no     CHAR(4)         NOT NULL,
            from_date   DATE            NOT NULL,
            to_date     DATE            NOT NULL,
            FOREIGN KEY (emp_no)  REFERENCES employees (emp_no)    ON DELETE CASCADE,
            FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
            PRIMARY KEY (emp_no,dept_no)
        );

        CREATE TABLE titles (
            emp_no      INT             NOT NULL,
            title       VARCHAR(50)     NOT NULL,
            from_date   DATE            NOT NULL,
            to_date     DATE,
            FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
            PRIMARY KEY (emp_no,title, from_date)
        ) 
        ; 

        CREATE TABLE salaries (
            emp_no      INT             NOT NULL,
            salary      INT             NOT NULL,
            from_date   DATE            NOT NULL,
            to_date     DATE            NOT NULL,
            FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
            PRIMARY KEY (emp_no, from_date)
        ) 
        ;
        """
        
        print("Creating table structures in `defaultdb`...")
        for cmd in schema_sql.split(';'):
            if cmd.strip():
                cursor.execute(cmd)
        conn.commit()
        
        # 2. Upload data files
        dump_files = [
            'load_departments.dump',
            'load_employees.dump',
            'load_dept_emp.dump',
            'load_dept_manager.dump',
            'load_titles.dump',
            'load_salaries1.dump',
            'load_salaries2.dump',
            'load_salaries3.dump'
        ]
        
        base_dir = os.path.join("test_db", "test_db-master")
        
        for file in dump_files:
            filepath = os.path.join(base_dir, file)
            print(f"Uploading {file} ... (this may take a few minutes)")
            try:
                execute_sql_file(cursor, filepath)
                conn.commit()
            except FileNotFoundError:
                print(f"WARNING: Could not find {filepath}. Skipping.")
        
        cursor.execute("SELECT COUNT(*) FROM employees")
        count = cursor.fetchone()[0]
        print(f"\nUpload Complete! Successfully loaded {count} employees into Aiven.")
        
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
