from collections import defaultdict
import logging
from modules.query_parser import QueryParser
from modules.fingerprint import fingerprint


class WorkloadAnalyzer:
    def __init__(self, metadata):
        self.metadata = metadata
        self.parser = QueryParser()
        self.tables = set(r['TABLE_NAME'].lower() for r in metadata)

    def _reset(self):
        """Reset all counters so the same instance can be reused safely."""
        self.where_freq = defaultdict(int)
        self.join_freq = defaultdict(int)
        self.groupby_freq = defaultdict(int)
        self.orderby_freq = defaultdict(int)
        self.query_count = 0
        self.anti_patterns = {
            'select_star': 0,
            'leading_wildcard': 0,
            'order_by_rand': 0,
            'missing_where': 0,
            'not_in_subquery': 0,
            'implicit_cross_join': 0,
            'function_on_col_where': 0,
            'offset_no_limit': 0,
            'cartesian_product': 0,
        }

    def analyze(self, query_source, progress_callback=None):
        """
        Analyze a query workload.

        Parameters
        ----------
        query_source : iterable (list or generator)
            SQL strings to analyze.  Works with a generator for streaming,
            keeping memory constant for huge files.
        progress_callback : callable(done, total) or None
            Called every 1 000 queries with (processed_count, -1).
            Use -1 as total when the total is unknown (generator).

        Returns
        -------
        dict with aggregated statistics.
        """
        self._reset()
        checkpoint = 5000 # Increased from 1000 for fewer SSE messages
        ast_cache = {}

        for q in query_source:
            if not q or not q.strip():
                continue

            self.query_count += 1
            
            fp = fingerprint(q)
            if fp in ast_cache:
                pq = ast_cache[fp]
            else:
                pq = self.parser.parse(q)
                if len(ast_cache) < 20000: # Cap cache size to avoid memory bloat
                    ast_cache[fp] = pq

            for col in pq.where_cols:
                self.where_freq[col] += 1
            for col in pq.join_cols:
                self.join_freq[col] += 1
            for col in pq.group_by:
                self.groupby_freq[col] += 1
            for col in pq.order_by:
                self.orderby_freq[col] += 1

            if pq.has_select_star:           self.anti_patterns['select_star'] += 1
            if pq.has_leading_wildcard:      self.anti_patterns['leading_wildcard'] += 1
            if pq.has_order_by_rand:         self.anti_patterns['order_by_rand'] += 1
            if pq.missing_where_clause:      self.anti_patterns['missing_where'] += 1
            if pq.has_not_in_subquery:       self.anti_patterns['not_in_subquery'] += 1
            if pq.has_implicit_cross_join:   self.anti_patterns['implicit_cross_join'] += 1
            if pq.has_function_on_col_where: self.anti_patterns['function_on_col_where'] += 1
            if pq.has_offset_no_limit:       self.anti_patterns['offset_no_limit'] += 1
            if pq.has_cartesian_product:     self.anti_patterns['cartesian_product'] += 1

            # Progress callback every 1 000 queries
            if progress_callback and self.query_count % checkpoint == 0:
                try:
                    progress_callback(self.query_count)
                except Exception:
                    pass

        if progress_callback:
            try:
                progress_callback(self.query_count)
            except Exception:
                pass

        logging.info(f"Workload analysis completed. {self.query_count} queries processed.")

        return {
            'where':        dict(self.where_freq),
            'join':         dict(self.join_freq),
            'groupby':      dict(self.groupby_freq),
            'orderby':      dict(self.orderby_freq),
            'anti_patterns': self.anti_patterns,
            'total_queries': self.query_count
        }
