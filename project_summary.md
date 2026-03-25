# Project Summary: DB Tuner Pro

**DB Tuner Pro** is a high-performance, cyberpunk-themed database workload analyzer and index recommendation tool. It helps database administrators and developers optimize their SQL databases by analyzing query patterns, performance bottlenecks, and suggesting ideal indexing strategies.

---

## 🛠️ Tech Stack
- **Backend:** Python (Flask)
- **Frontend:** Vanilla HTML5, CSS3 (Modern Cyberpunk Design), JavaScript (ES6+)
- **Data Visualization:** Chart.js
- **Database Support:** MySQL, MariaDB, PostgreSQL (via adapters like `mysql-connector-python` and `psycopg2`)
- **Internal Storage:** SQLite (used for local project management and history)
- **Real-time Streaming:** Server-Sent Events (SSE) for live analysis progress

---

## 🏗️ Project Structure
```text
/
├── app.py                     # Main Flask server and API routes
├── config.yaml                # Global application and database configuration
├── requirements.txt           # Python dependencies (Flask, sqlglot, PyYAML, etc.)
├── projects.db                # SQLite database for project history
├── modules/                   # CORE LOGIC (Python)
│   ├── workload_analyzer.py   # Parses SQL and extracts WHERE/JOIN/GROUP BY clauses
│   ├── recommender.py         # Logic for suggesting optimal indexes
│   ├── evaluator.py           # Benchmarks current performance vs. suggested changes
│   ├── fingerprint.py         # Groups similar queries into unique templates
│   ├── explain_parser.py      # Parses visual EXPLAIN plans from MySQL/Postgres
│   └── db_connector.py        # Generic interface for targeting multiple DB types
├── static/                    # FRONTEND ASSETS
│   ├── css/style.css          # The "Cyberpunk UI" system (Neon, Glassmorphism, Grids)
│   └── js/
│       ├── app.js             # Core state management, SSE handling, and Chart.js logic
│       ├── cyber-effects.js   # UI animations (Particle canvas, 3D card tilt, magnetic buttons)
│       └── gradual-blur.js    # Visual utility for focused scrolling
└── templates/                 # JINJA2 HTML TEMPLATES
    ├── base.html              # Main layout shell with global headers and sidebars
    ├── home.html              # Project selection and connection screen
    └── index.html             # The main analysis dashboard
```

---

## 🚀 Key Functionalities
1. **Workload Analysis:** Reads large SQL files or database logs and uses `sqlglot` to parse every query to identify which columns are most frequently filtered, joined, or sorted.
2. **Index Recommendations:** An intelligent algorithm looks at column frequency and cardinality to suggest `CREATE INDEX` statements that would provide the highest performance uplift.
3. **Live Benchmarking:** Uses Server-Sent Events (SSE) to stream analysis progress to the UI in real-time, allowing users to see progress numbers even for workloads with millions of queries.
4. **Query Fingerprinting:** Automatically replaces literals in SQL queries (e.g., `WHERE id=5` -> `WHERE id=?`) to group thousands of similar queries into a single "fingerprint" for easier performance tracking.
5. **Interactive Dashboard:** Visualizes data distributions across tables and columns using radar charts, bar charts, and list views.
6. **Cyberpunk UI:** An immersive, futuristic user interface designed for 2024-ready aesthetics, featuring neon light-glows, particle backgrounds, and glassmorphism.

---

## 📖 How to Learn from this Codebase
- **In `app.py`:** Observe how `stream_with_context` is used to create smooth, non-blocking real-time updates for long-running analysis tasks.
- **In `modules/workload_analyzer.py`:** See how a production-grade SQL parser works to recursively traverse ASTs (Abstract Syntax Trees) to pull out table/column relationships.
- **In `static/js/app.js`:** Study the custom state management pattern that handles complex UI updates without a heavy framework like React.
- **In `static/css/style.css`:** Learn how modern CSS custom properties (variables) are used to build a cohesive design system from scratch.
