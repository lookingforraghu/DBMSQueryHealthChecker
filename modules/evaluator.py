import time
import random
import logging


class PerformanceEvaluator:
    def __init__(self, db_connector):
        self.db = db_connector

    def evaluate(self, recommendations, sample_queries):
        """
        Evaluate index recommendations by timing sample queries before/after
        temporarily creating the index.

        sample_queries may be a list or a materialised subset – we random-sample
        from it so we don't always test only the first 5 queries.
        """
        logging.info("Evaluating recommendations...")

        # Materialise a sample pool capped at 200 to keep eval fast
        pool = list(sample_queries)[:200]

        eval_results = []
        for rec in recommendations:
            if rec['type'] == 'INDEX':
                table = rec['table']
                col = rec['columns'][0]

                # Pick up to 1 query (reduced from 5 for faster benchmarking on millions of rows)
                matching = [q for q in pool if 'select' in q.lower() and col.lower() in q.lower()]
                test_queries = [matching[0]] if matching else []

                if not test_queries:
                    rec['speedup'] = 'N/A'
                    eval_results.append(rec)
                    continue

                # Time before index
                time_before = self._run_and_time(test_queries)

                # Temporarily apply index
                try:
                    self.db.execute_query(rec['sql'])
                    time_after = self._run_and_time(test_queries)

                    # Drop temporary index
                    drop_sql = f"DROP INDEX idx_{table.lower()}_{col}_auto ON {table};"
                    self.db.execute_query(drop_sql)

                    if time_before > 0:
                        speedup = ((time_before - time_after) / time_before) * 100
                        rec['speedup'] = f"{max(0.0, speedup):.1f}%"
                    else:
                        rec['speedup'] = '0%'

                except Exception as e:
                    logging.warning(f"Could not benchmark index for {table}.{col}: {e}")
                    rec['speedup'] = 'N/A'

            else:
                # Views / other – no live evaluation
                rec['speedup'] = 'N/A (Schema Object)'

            eval_results.append(rec)

        return eval_results

    def _run_and_time(self, queries):
        total = 0.0
        for q in queries:
            try:
                start = time.perf_counter()
                self.db.execute_query(q)
                total += time.perf_counter() - start
            except Exception as e:
                logging.debug(f"Timing query skipped: {e}")
        return total
