"""
samples.py
Pre-built benchmark queries for the MySQL Employees database.
These queries showcase full table scans, complex joins, and efficient index
usage to demonstrate realistic performance analysis.

Endpoint: GET /api/sample-queries
"""

EMPLOYEE_SAMPLES = [
    {
        "id": "full_scan_salary",
        "name": "Full Table Scan — Salaries",
        "badge": "critical",
        "description": (
            "Queries the 2.8M-row salaries table with an unindexed filter "
            "on the salary column. Expect MySQL EXPLAIN to report type=ALL "
            "and scan severity as Critical."
        ),
        "queries": [
            "SELECT emp_no, salary FROM salaries WHERE salary > 80000",
            "SELECT emp_no, salary, from_date FROM salaries WHERE salary BETWEEN 60000 AND 90000",
            "SELECT COUNT(*) FROM salaries WHERE salary > 100000",
            "SELECT emp_no, MAX(salary) FROM salaries GROUP BY emp_no HAVING MAX(salary) > 90000",
            "SELECT s.emp_no, s.salary FROM salaries s WHERE s.salary > 120000 ORDER BY s.salary DESC",
        ]
    },
    {
        "id": "join_heavy",
        "name": "Join-Heavy — Salary + Dept + Title",
        "badge": "warning",
        "description": (
            "Three-way JOIN across employees, dept_emp, and salaries. "
            "Demonstrates how unindexed columns in join conditions multiply "
            "row scans exponentially."
        ),
        "queries": [
            """SELECT e.first_name, e.last_name, d.dept_no, s.salary
               FROM employees e
               JOIN dept_emp d ON e.emp_no = d.emp_no
               JOIN salaries s ON e.emp_no = s.emp_no
               WHERE d.to_date = '9999-01-01' AND s.to_date = '9999-01-01'""",

            """SELECT e.first_name, e.last_name, t.title, s.salary
               FROM employees e
               JOIN titles t      ON e.emp_no = t.emp_no
               JOIN salaries s    ON e.emp_no = s.emp_no
               JOIN dept_emp de   ON e.emp_no = de.emp_no
               WHERE t.to_date = '9999-01-01' AND s.to_date = '9999-01-01'""",

            """SELECT dept_no, AVG(salary) as avg_salary, COUNT(*) as cnt
               FROM dept_emp JOIN salaries USING (emp_no)
               WHERE dept_emp.to_date > '2000-01-01'
               GROUP BY dept_no
               ORDER BY avg_salary DESC""",

            """SELECT e.gender, COUNT(*) as cnt, AVG(s.salary) as avg_sal
               FROM employees e JOIN salaries s ON e.emp_no = s.emp_no
               WHERE s.to_date = '9999-01-01'
               GROUP BY e.gender""",
        ]
    },
    {
        "id": "anti_patterns",
        "name": "Anti-Pattern Showcase",
        "badge": "warning",
        "description": (
            "Queries using SELECT *, ORDER BY RAND(), functions in WHERE, "
            "and NOT IN subqueries — classic anti-patterns that the analyzer "
            "should flag." 
        ),
        "queries": [
            "SELECT * FROM employees WHERE YEAR(hire_date) = 1999",
            "SELECT * FROM employees ORDER BY RAND() LIMIT 5",
            "SELECT * FROM salaries WHERE emp_no NOT IN (SELECT emp_no FROM dept_manager)",
            "SELECT * FROM employees WHERE first_name LIKE '%son'",
            "UPDATE salaries SET salary = salary * 1.1",
            "SELECT * FROM employees, dept_emp WHERE employees.emp_no = dept_emp.emp_no",
        ]
    },
    {
        "id": "optimized",
        "name": "Optimised — Index-backed Queries",
        "badge": "ok",
        "description": (
            "Queries that use primary-key or indexed lookups. EXPLAIN should "
            "show type=ref or const with low row estimates. Compare these "
            "against the full-scan queries above."
        ),
        "queries": [
            "SELECT emp_no, first_name, last_name, hire_date FROM employees WHERE emp_no = 10001",
            "SELECT emp_no, salary, from_date FROM salaries WHERE emp_no = 10001",
            "SELECT emp_no, dept_no, from_date FROM dept_emp WHERE emp_no BETWEEN 10001 AND 10100",
            "SELECT emp_no, title FROM titles WHERE emp_no = 10001 AND to_date = '9999-01-01'",
            "SELECT dept_no, dept_name FROM departments WHERE dept_no = 'd001'",
        ]
    }
]


def get_samples():
    """
    Return the sample query groups as a list of dicts.
    The queries list is joined into a single workload string
    that can be saved to a temp file for streaming analysis.
    """
    return EMPLOYEE_SAMPLES


def get_flat_workload(group_id: str = None) -> str:
    """
    Return all sample queries (or a specific group) as a
    newline-separated, semicolon-terminated string ready to
    be written to a temp .txt file and fed into the analyzer.
    """
    groups = EMPLOYEE_SAMPLES if not group_id else [
        g for g in EMPLOYEE_SAMPLES if g['id'] == group_id
    ]
    lines = []
    for g in groups:
        lines.append(f"-- {g['name']}")
        for q in g['queries']:
            # normalise whitespace and end with ;
            clean = ' '.join(q.split()).rstrip(';')
            lines.append(clean + ';')
        lines.append('')
    return '\n'.join(lines)
