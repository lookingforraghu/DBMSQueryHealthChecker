import sqlglot
import sqlglot.expressions as exp
import re
import logging

class ParsedQuery:
    def __init__(self, raw_sql):
        self.raw_sql = raw_sql
        self.tables = set()
        self.where_cols = []
        self.join_cols = []
        self.group_by = []
        self.order_by = []

        # Anti-pattern flags
        self.has_select_star = False
        self.has_leading_wildcard = False
        self.has_order_by_rand = False
        self.missing_where_clause = False
        # New anti-patterns
        self.has_not_in_subquery = False
        self.has_implicit_cross_join = False
        self.has_function_on_col_where = False
        self.has_offset_no_limit = False
        self.has_cartesian_product = False


class QueryParser:
    def __init__(self):
        pass

    def parse(self, sql):
        if not sql or not sql.strip():
            return ParsedQuery(sql or '')

        parsed = ParsedQuery(sql)
        sql_lower = sql.lower()

        try:
            # Parse with sqlglot for robust AST traversal
            ast_list = sqlglot.parse(sql, read="mysql")
            if ast_list and ast_list[0]:
                ast = ast_list[0]

                # 1. Extract Tables
                for t in ast.find_all(exp.Table):
                    if t.name:
                        parsed.tables.add(t.name)

                # 2. Extract WHERE columns
                for w in ast.find_all(exp.Where):
                    for c in w.find_all(exp.Column):
                        if c.name:
                            parsed.where_cols.append(c.name)

                # 3. Extract JOIN columns
                for j in ast.find_all(exp.Join):
                    on = j.args.get('on')
                    if on:
                        for c in on.find_all(exp.Column):
                            if c.name:
                                parsed.join_cols.append(c.name)

                # 4. Extract GROUP BY
                for g in ast.find_all(exp.Group):
                    for c in g.find_all(exp.Column):
                        if c.name:
                            parsed.group_by.append(c.name)

                # 5. Extract ORDER BY
                for o in ast.find_all(exp.Order):
                    for c in o.find_all(exp.Column):
                        if c.name:
                            parsed.order_by.append(c.name)

                # --- ANTI-PATTERN DETECTION (SQLGLOT) ---
                if any(ast.find_all(exp.Star)):
                    parsed.has_select_star = True

                for l in ast.find_all(exp.Like):
                    right = l.expression
                    if isinstance(right, exp.Literal) and right.name.startswith('%'):
                        parsed.has_leading_wildcard = True

                for o in ast.find_all(exp.Order):
                    if any(o.find_all(exp.Rand)):
                        parsed.has_order_by_rand = True

                if isinstance(ast, (exp.Update, exp.Delete)):
                    if not ast.args.get("where"):
                        parsed.missing_where_clause = True

                for w in ast.find_all(exp.Where):
                    for f in w.find_all(exp.Func):
                        if any(f.find_all(exp.Column)):
                            parsed.has_function_on_col_where = True

                if ast.args.get("offset") and not ast.args.get("limit"):
                    parsed.has_offset_no_limit = True
                
                for i in ast.find_all(exp.In):
                    if getattr(i, "query", None) or i.args.get("query"):
                        # If it's a subquery, we check if it's NOT IN
                        if isinstance(i.parent, exp.Not) or i.args.get("not") or getattr(i, "not_", False):
                            parsed.has_not_in_subquery = True

        except Exception as e:
            logging.warning(f"QueryParser (sqlglot): failed to parse query – {e}")
            pass
        
        # --- ANTI-PATTERN DETECTION (REGEX FALLBACK) ---
        # Some things are easier/safer to catch with regex to maintain 100% backward compatibility
        try:
            # 5. NOT IN (subquery)
            if not parsed.has_not_in_subquery and re.search(r'\bnot\s+in\s*\(\s*select\b', sql_lower):
                parsed.has_not_in_subquery = True

            # 6. Implicit cross join (FROM t1, t2 without JOIN / WHERE)
            from_match = re.search(r'\bfrom\b([^where^join]+)', sql_lower)
            if from_match:
                from_clause = from_match.group(1)
                comma_tables = [t.strip() for t in from_clause.split(',') if t.strip()]
                if len(comma_tables) > 1 and 'join' not in sql_lower:
                    parsed.has_implicit_cross_join = True

            # 9. Cartesian product
            if re.search(r'\bselect\b', sql_lower):
                if 'join' not in sql_lower and 'where' not in sql_lower:
                    from_m = re.search(r'\bfrom\b(.+?)(?:group|order|limit|$)', sql_lower)
                    if from_m:
                        from_tbls = [t.strip() for t in from_m.group(1).split(',') if t.strip()]
                        if len(from_tbls) > 1:
                            parsed.has_cartesian_product = True
        except Exception as e:
             logging.warning(f"QueryParser (regex): failed to evaluate anti-patterns – {e}")

        return parsed
