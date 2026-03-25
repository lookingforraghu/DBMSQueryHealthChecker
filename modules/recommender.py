import logging
from collections import defaultdict
from modules.cost_estimator import CostEstimator

# Maximum index recommendations to return (no one can act on hundreds)
MAX_INDEX_RECS = 50
# Minimum benefit score before recommending an index
INDEX_THRESHOLD = 30
# Minimum GROUP BY frequency before suggesting a materialized view
VIEW_THRESHOLD = 3


class Recommender:
    def __init__(self, schema_metadata, existing_indexes):
        self.metadata = schema_metadata
        self.existing_indexes_set = set()
        for idx in existing_indexes:
            self.existing_indexes_set.add(
                f"{idx['TABLE_NAME'].lower()}.{idx['COLUMN_NAME'].lower()}"
            )
        self.estimator = CostEstimator(schema_metadata)

    def generate_recommendations(self, stats):
        recommendations = []

        # Build column → list[table] mapping (column names can appear in multiple tables)
        col_to_tables = defaultdict(list)
        for row in self.metadata:
            col_to_tables[row['COLUMN_NAME'].lower()].append(row['TABLE_NAME'])

        def is_indexed(table, col):
            return f"{table.lower()}.{col.lower()}" in self.existing_indexes_set

        # ─── Index recommendations ────────────────────────────────────────
        scored = []
        for row in self.metadata:
            col   = row['COLUMN_NAME'].lower()
            table = row['TABLE_NAME']
            score = self.estimator.estimate_benefit(col, stats)
            if score >= INDEX_THRESHOLD and not is_indexed(table, col):
                scored.append((score, col, table, row))

        # Sort by descending score so we surface the most impactful first
        scored.sort(key=lambda x: x[0], reverse=True)

        for score, col, table, row in scored[:MAX_INDEX_RECS]:
            maintenance_cost = 'Low' if score < 100 else 'Medium'

            explanation_parts = []
            reason_parts = []

            w_freq = stats['where'].get(col, 0)
            j_freq = stats['join'].get(col, 0)

            if w_freq > 0:
                reason_parts.append(f"high-frequency filtering ({w_freq}×)")
                explanation_parts.append(
                    f"The workload filters on `{col}` {w_freq} times. "
                    f"A B-Tree index allows the engine to jump directly to matching rows "
                    f"instead of scanning the entire `{table}` table."
                )
            if j_freq > 0:
                reason_parts.append(f"join usage ({j_freq}×)")
                explanation_parts.append(
                    f"The workload joins on `{col}` {j_freq} times. "
                    f"An index here eliminates expensive nested-loop full scans at join time."
                )

            reason_str      = ' and '.join(reason_parts) if reason_parts else 'frequent usage'
            explanation_str = ' '.join(explanation_parts) if explanation_parts else \
                              'An index is recommended to speed up lookups on this column.'

            recommendations.append({
                'type':             'INDEX',
                'table':            table,
                'columns':          [col],
                'index_type':       'B-Tree Index',
                'reason':           reason_str,
                'explanation':      explanation_str,
                'maintenance_cost': maintenance_cost,
                'sql':              f"CREATE INDEX idx_{table.lower()}_{col}_auto ON {table}({col});",
                'score':            score,
            })

        PARTITION_THRESHOLD = 20
        # ─── Table Partitioning recommendations ───────
        for row in self.metadata:
            col   = row['COLUMN_NAME'].lower()
            table = row['TABLE_NAME']
            dtype = row.get('DATA_TYPE', '').lower()
            w_freq = stats['where'].get(col, 0)
            
            if w_freq >= PARTITION_THRESHOLD and dtype in ('date', 'datetime', 'timestamp'):
                recommendations.append({
                    'type':             'PARTITION',
                    'table':            table,
                    'columns':          [col],
                    'index_type':       'Range Partitioning',
                    'reason':           f"Heavy date-range filtering on `{col}` ({w_freq}×)",
                    'explanation':      (
                        f"The workload frequently filters on the date column `{col}` ({w_freq} times). "
                        f"For massive enterprise tables, partitioning the table by RANGE "
                        f"significantly reduces IO scanning compared to just indexing."
                    ),
                    'maintenance_cost': 'High',
                    'sql': (
                        f"ALTER TABLE {table} PARTITION BY RANGE (YEAR({col})) ("
                        f"PARTITION p0 VALUES LESS THAN (2023), "
                        f"PARTITION p1 VALUES LESS THAN (2024), "
                        f"PARTITION p2 VALUES LESS THAN MAXVALUE);"
                    ),
                    'score':            min(100, w_freq)
                })

        # ─── Materialized-view (simulated via VIEW) recommendations ───────
        for col, count in stats['groupby'].items():
            if count >= VIEW_THRESHOLD and col in col_to_tables:
                for table in col_to_tables[col]:
                    recommendations.append({
                        'type':             'VIEW',
                        'table':            table,
                        'columns':          [col],
                        'index_type':       'Materialized View (Simulated)',
                        'reason':           f"Heavy aggregation grouped by `{col}` ({count}×)",
                        'explanation':      (
                            f"The workload repeatedly groups on `{col}` ({count} times). "
                            f"A materialized view pre-computes these aggregations, saving "
                            f"significant CPU time for reporting and dashboard queries."
                        ),
                        'maintenance_cost': 'High',
                        'sql': (
                            f"CREATE VIEW v_{table.lower()}_{col}_summary AS "
                            f"SELECT {col}, COUNT(*) AS cnt FROM {table} GROUP BY {col};"
                        ),
                    })

        return recommendations
