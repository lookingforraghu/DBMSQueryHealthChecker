"""
fingerprint.py
Normalizes SQL queries into canonical "fingerprints" (templates) by replacing
all literal values with a placeholder, then groups an entire workload by those
templates to compute cumulative statistics.

This is how production APM tools (New Relic, Datadog, PMM) detect the real
costliest queries: not by looking at individual executions, but by aggregating
thousands of parametrized variants under one template.

Example:
    "SELECT * FROM salaries WHERE emp_no = 10001"
    "SELECT * FROM salaries WHERE emp_no = 99999"
    → both become: "SELECT * FROM salaries WHERE emp_no = ?"

    Template execution count: 2
    Cumulative rows examined: 2 × (whatever EXPLAIN says)
"""

import re
import hashlib
import logging
from collections import defaultdict


# ── Regex patterns for value normalisation ─────────────────────────────────────
_PATTERNS = [
    # Single-quoted string literals (handles escaped quotes inside)
    (re.compile(r"'(?:[^'\\]|\\.)*'"), "'?'"),
    # Double-quoted string literals
    (re.compile(r'"(?:[^"\\]|\\.)*"'), '"?"'),
    # Hex literals  0x1A2B
    (re.compile(r'\b0x[0-9a-fA-F]+\b'), '?'),
    # Floating-point numbers (must come before integers)
    (re.compile(r'\b\d+\.\d+\b'), '?'),
    # Integer literals
    (re.compile(r'\b\d+\b'), '?'),
    # IN (...) lists of values → IN (?)  so the list length doesn't create new templates
    (re.compile(r'\bIN\s*\(\s*\?[\s,\?]*\)', re.IGNORECASE), 'IN (?)'),
    # LIMIT / OFFSET numbers (already caught by integer pattern but be explicit)
    (re.compile(r'\bLIMIT\s+\?(?:\s*,\s*\?)?\b', re.IGNORECASE), 'LIMIT ?'),
    # Collapse whitespace
    (re.compile(r'\s+'), ' '),
]


def fingerprint(sql: str) -> str:
    """
    Return the normalised fingerprint of a SQL query.
    The result is safe to use as a dict key.
    """
    text = sql.strip()
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text.strip()


def fingerprint_id(fp: str) -> str:
    """Return an 8-char hex ID for a fingerprint (for display)."""
    return hashlib.md5(fp.encode()).hexdigest()[:8]


# ── Workload-level aggregation ─────────────────────────────────────────────────

class FingerprintGroup:
    """Aggregated statistics for one query template."""

    def __init__(self, template: str):
        self.template      = template
        self.fid           = fingerprint_id(template)
        self.count         = 0
        self.rows_total    = 0       # sum of EXPLAIN rows_est across all executions
        self.severity      = 'ok'    # worst severity seen for this template
        self.scan_type     = None    # worst scan type seen
        self.example       = None    # one real query from the workload

    def record(self, original_query: str, explain_result=None):
        self.count += 1
        if self.example is None:
            self.example = original_query

        if explain_result:
            rows = explain_result.get('rows_est') or 0
            try:
                self.rows_total += int(rows)
            except (TypeError, ValueError):
                pass

            sev = explain_result.get('severity', 'ok')
            sev_rank = {'critical': 0, 'warning': 1, 'ok': 2}
            if sev_rank.get(sev, 2) < sev_rank.get(self.severity, 2):
                self.severity  = sev
                self.scan_type = explain_result.get('scan_type')

    def to_dict(self) -> dict:
        return {
            'fid':         self.fid,
            'template':    self.template,
            'count':       self.count,
            'rows_total':  self.rows_total,
            'rows_avg':    round(self.rows_total / self.count) if self.count else 0,
            'severity':    self.severity,
            'scan_type':   self.scan_type,
            'example':     self.example,
        }


class WorkloadFingerprinter:
    """
    Accumulate per-query EXPLAIN results and expose them as
    sorted, aggregated FingerprintGroup objects.
    """

    def __init__(self):
        self._groups: dict[str, FingerprintGroup] = {}

    def record(self, sql: str, explain_result: dict = None):
        fp = fingerprint(sql)
        if fp not in self._groups:
            self._groups[fp] = FingerprintGroup(fp)
        self._groups[fp].record(sql, explain_result)

    def top_groups(self, n: int = 20, sort_by: str = 'rows_total') -> list:
        """
        Return top-n groups sorted by cumulative rows examined (default)
        or by execution count.
        """
        groups = list(self._groups.values())
        groups.sort(key=lambda g: getattr(g, sort_by, 0), reverse=True)
        return [g.to_dict() for g in groups[:n]]

    def summary(self) -> dict:
        all_groups = list(self._groups.values())
        total_exec = sum(g.count for g in all_groups)
        total_rows = sum(g.rows_total for g in all_groups)
        critical   = sum(1 for g in all_groups if g.severity == 'critical')
        return {
            'unique_templates': len(all_groups),
            'total_executions': total_exec,
            'total_rows_est':   total_rows,
            'critical_templates': critical,
        }
