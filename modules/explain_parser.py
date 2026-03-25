"""
explain_parser.py
Parses raw EXPLAIN output from MySQL or PostgreSQL and extracts
cost, scan type, index used, and severity for the UI.
"""

import re
import logging


# ── Severity map for MySQL "type" column ───────────────────────────────────────
# https://dev.mysql.com/doc/refman/8.0/en/explain-output.html#explain-join-types
MYSQL_SCAN_SEVERITY = {
    'ALL':    'critical',   # full table scan
    'index':  'warning',    # full index scan (all index rows)
    'range':  'ok',         # range scan
    'ref':    'ok',
    'eq_ref': 'ok',
    'const':  'ok',
    'system': 'ok',
    'NULL':   'ok',
}

POSTGRES_BAD_SCANS = {'Seq Scan'}


class ExplainResult:
    def __init__(self):
        self.scan_type = 'Unknown'
        self.key_used  = None         # index name (MySQL), or None
        self.rows_est  = None         # estimated rows examined
        self.cost      = None         # estimated cost (PostgreSQL) or rows (MySQL proxy)
        self.severity  = 'ok'         # 'ok' | 'warning' | 'critical'
        self.table     = None

    def to_dict(self):
        return {
            'scan_type': self.scan_type,
            'key_used':  self.key_used,
            'rows_est':  self.rows_est,
            'cost':      self.cost,
            'severity':  self.severity,
            'table':     self.table,
        }


class ExplainParser:
    """
    Usage:
        rows = db.get_explain_plan(query)   # list of dicts
        result = ExplainParser.parse(rows, engine='mysql')
    """

    @staticmethod
    def parse(rows: list, engine: str = 'mysql') -> ExplainResult:
        if engine.startswith('mysql'):
            return ExplainParser._parse_mysql(rows)
        else:
            return ExplainParser._parse_postgres(rows)

    # ── MySQL ──────────────────────────────────────────────────────────────────
    @staticmethod
    def _parse_mysql(rows: list) -> ExplainResult:
        result = ExplainResult()
        if not rows:
            return result

        # Look for the worst row — i.e., the row with the worst scan type
        priority = list(MYSQL_SCAN_SEVERITY.keys())

        worst_row   = None
        worst_rank  = len(priority)  # lower = worse

        for row in rows:
            scan = str(row.get('type') or row.get('TYPE') or 'NULL')
            rank = priority.index(scan) if scan in priority else len(priority) - 1
            if rank < worst_rank:
                worst_rank = rank
                worst_row  = row

        if worst_row is None:
            worst_row = rows[0]

        scan = str(worst_row.get('type') or worst_row.get('TYPE') or 'NULL')
        result.scan_type = scan
        result.key_used  = worst_row.get('key') or worst_row.get('KEY') or None
        result.rows_est  = worst_row.get('rows') or worst_row.get('ROWS')
        result.table     = worst_row.get('table') or worst_row.get('TABLE')
        result.severity  = MYSQL_SCAN_SEVERITY.get(scan, 'warning')

        # Use rows as cost proxy (higher rows → higher "cost")
        try:
            result.cost = f"{int(result.rows_est):,} rows est."
        except Exception:
            result.cost = str(result.rows_est)

        return result

    # ── PostgreSQL ─────────────────────────────────────────────────────────────
    @staticmethod
    def _parse_postgres(rows: list) -> ExplainResult:
        result = ExplainResult()
        if not rows:
            return result

        # rows is a list of {'QUERY PLAN': '...'} lines
        plan_text = '\n'.join(
            str(r.get('QUERY PLAN') or r.get('query plan') or '') for r in rows
        )

        # Detect scan type
        if 'Seq Scan' in plan_text:
            result.scan_type = 'Seq Scan'
            result.severity  = 'critical'
        elif 'Index Scan' in plan_text:
            result.scan_type = 'Index Scan'
            result.severity  = 'ok'
        elif 'Bitmap Heap Scan' in plan_text:
            result.scan_type = 'Bitmap Heap Scan'
            result.severity  = 'ok'
        else:
            result.scan_type = 'Other'

        # Extract cost e.g. (cost=0.00..1234.56 ...)
        m = re.search(r'cost=[\d.]+\.\.([\d.]+)', plan_text)
        if m:
            result.cost = m.group(1)

        # Extract rows e.g. rows=302533
        m = re.search(r'rows=(\d+)', plan_text)
        if m:
            result.rows_est = int(m.group(1))
            result.cost     = f"~{int(m.group(1)):,} rows est."

        return result
