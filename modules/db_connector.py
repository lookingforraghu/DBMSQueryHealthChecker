import mysql.connector
from mysql.connector import Error as MySQLError
import psycopg2
from psycopg2 import Error as Psycopg2Error
import psycopg2.extras
import logging
import re

class DBConnector:
    def __init__(self, config):
        self.config = config['database']
        # Default to mysql if engine is not specified
        self.engine = self.config.get('engine', 'mysql').lower()
        self.connection = None

    def connect(self):
        try:
            if self.engine == 'mysql':
                self.connection = mysql.connector.connect(
                    host=self.config.get('host', 'localhost'),
                    port=self.config.get('port', 3306),
                    user=self.config.get('user', 'root'),
                    password=self.config.get('password', ''),
                    database=self.config.get('database')
                )
            elif self.engine in ('postgres', 'postgresql'):
                self.connection = psycopg2.connect(
                    host=self.config.get('host', 'localhost'),
                    port=self.config.get('port', 5432),
                    user=self.config.get('user', 'postgres'),
                    password=self.config.get('password', ''),
                    dbname=self.config.get('database')
                )
            else:
                raise ValueError(f"Unsupported database engine: {self.engine}")
            logging.info(f"Connected to {self.engine} successfully.")
        except Exception as e:
            logging.error(f"Error connecting to {self.engine}: {e}")
            raise

    def close(self):
        if self.connection:
            if self.engine == 'mysql' and self.connection.is_connected():
                self.connection.close()
            elif self.engine in ('postgres', 'postgresql') and self.connection.closed == 0:
                self.connection.close()
            logging.info(f"{self.engine} connection closed.")
            self.connection = None

    def execute_query(self, query, params=None):
        if not self.connection:
            self.connect()
        else:
            if self.engine == 'mysql' and not self.connection.is_connected():
                self.connect()
            elif self.engine in ('postgres', 'postgresql') and self.connection.closed != 0:
                self.connect()

        try:
            if self.engine == 'mysql':
                cursor = self.connection.cursor(dictionary=True)
            else:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
            cursor.execute(query, params)
            upper = query.strip().upper()
            if upper.startswith("SELECT") or upper.startswith("EXPLAIN") or upper.startswith("SHOW"):
                res = cursor.fetchall()
                if self.engine in ('postgres', 'postgresql'):
                    # Convert psycopg2 RealDictRow to standard dict for compatibility
                    res = [dict(r) for r in res]
            else:
                self.connection.commit()
                res = []
            return res
        except Exception as e:
            if self.connection and self.engine in ('postgres', 'postgresql'):
                self.connection.rollback()
            logging.error(f"Query Error '{query[:120]}': {e}")
            raise
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()

    def get_schema_metadata(self):
        if self.engine == 'mysql':
            query = """
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
            """
            return self.execute_query(query, (self.config['database'],))
        elif self.engine in ('postgres', 'postgresql'):
            # PostgreSQL equivalent
            query = """
                SELECT 
                    table_name AS "TABLE_NAME", 
                    column_name AS "COLUMN_NAME", 
                    data_type AS "DATA_TYPE", 
                    '' AS "COLUMN_KEY"
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_catalog = %s
            """
            return self.execute_query(query, (self.config['database'],))

    def get_indexes(self):
        if self.engine == 'mysql':
            query = """
                SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s
            """
            return self.execute_query(query, (self.config['database'],))
        elif self.engine in ('postgres', 'postgresql'):
            query = """
                SELECT
                    t.relname AS "TABLE_NAME",
                    i.relname AS "INDEX_NAME",
                    a.attname AS "COLUMN_NAME",
                    CASE WHEN ix.indisunique THEN 0 ELSE 1 END AS "NON_UNIQUE"
                FROM
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a,
                    pg_namespace n
                WHERE
                    t.oid = ix.indrelid
                    AND i.oid = ix.indexrelid
                    AND a.attrelid = t.oid
                    AND a.attnum = ANY(ix.indkey)
                    AND t.relnamespace = n.oid
                    AND n.nspname = 'public'
                    AND t.relkind = 'r'
            """
            return self.execute_query(query)

    def get_explain_plan(self, query: str) -> list:
        """
        Run EXPLAIN on a single query and return the raw rows.
        Uses EXPLAIN (not EXPLAIN ANALYZE) so it is always safe to call on
        SELECT/UPDATE/DELETE without modifying data.
        """
        try:
            # Only EXPLAIN SELECT-like statements to avoid side effects
            stripped = query.strip().upper()
            if not any(stripped.startswith(k) for k in ('SELECT', 'UPDATE', 'DELETE', 'INSERT')):
                return []

            if self.engine == 'mysql':
                explain_sql = f"EXPLAIN {query}"
            else:
                explain_sql = f"EXPLAIN {query}"

            rows = self.execute_query(explain_sql)
            # Normalise keys to lowercase for consistency
            return [{k.lower(): v for k, v in row.items()} for row in rows]
        except Exception as e:
            logging.warning(f"EXPLAIN failed for query: {e}")
            return []

    def test_connection(self) -> bool:
        """Lightweight check — connect and immediately disconnect."""
        try:
            self.connect()
            return True
        except Exception:
            return False
        finally:
            self.close()

    # ─── Streaming workload reader ─────────────────────────────────────────

    def fetch_workload(self, source, query_file=None):
        if source == "file" and query_file:
            return self._stream_file(query_file)
        elif source == "general_log":
            return self._fetch_general_log()
        elif source == "performance_schema":
            return self._fetch_perf_schema()
        return iter([])

    def _stream_file(self, query_file):
        VALID_STARTS = ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
        block_comment = False
        current = []

        try:
            with open(query_file, 'r', encoding='utf-8', errors='ignore') as f:
                for raw_line in f:
                    line = raw_line.strip()

                    # Handle block comments /* ... */
                    if block_comment:
                        if '*/' in line:
                            line = line[line.index('*/') + 2:].strip()
                            block_comment = False
                        else:
                            continue

                    if '/*' in line:
                        before = line[:line.index('/*')]
                        after_start = line[line.index('/*') + 2:]
                        if '*/' in after_start:
                            line = before + after_start[after_start.index('*/') + 2:]
                        else:
                            line = before
                            block_comment = True

                    if '--' in line:
                        line = line[:line.index('--')].strip()
                    if line.startswith('#'):
                        continue

                    if not line:
                        continue

                    if ';' in line:
                        parts = line.split(';')
                        for i, part in enumerate(parts):
                            part = part.strip()
                            if i < len(parts) - 1:
                                current.append(part)
                                stmt = ' '.join(current).strip()
                                current = []
                                if stmt and stmt.upper().startswith(VALID_STARTS):
                                    yield stmt
                            else:
                                if part:
                                    current.append(part)
                    else:
                        current.append(line)

                if current:
                    stmt = ' '.join(current).strip()
                    if stmt and stmt.upper().startswith(VALID_STARTS):
                        yield stmt

        except FileNotFoundError:
            logging.error(f"Workload file not found: {query_file}")
        except Exception as e:
            logging.error(f"Error reading workload file: {e}")

    def _fetch_general_log(self):
        if self.engine == 'mysql':
            q = "SELECT argument FROM mysql.general_log WHERE command_type = 'Query'"
        else:
            q = "SELECT query as argument FROM pg_stat_activity WHERE state = 'active'"
            
        try:
            res = self.execute_query(q)
            for r in res:
                arg = r['argument']
                if arg:
                    if isinstance(arg, bytes):
                        arg = arg.decode('utf-8', errors='ignore')
                    yield arg
        except Exception as e:
            logging.warning(f"Failed to fetch from DB log: {e}")

    def _fetch_perf_schema(self):
        if self.engine == 'mysql':
            q = "SELECT DIGEST_TEXT as q FROM performance_schema.events_statements_summary_by_digest WHERE DIGEST_TEXT IS NOT NULL"
        else:
            q = "SELECT query as q FROM pg_stat_statements"
            
        try:
            res = self.execute_query(q)
            for r in res:
                if r['q']:
                    yield r['q']
        except Exception as e:
            logging.warning(f"Failed to fetch from performance schema: {e}")
