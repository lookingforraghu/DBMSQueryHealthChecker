from flask import Flask, render_template, request, jsonify, session, stream_with_context, Response
import yaml
import logging
import json
import secrets
import sqlite3
import os
import tempfile
from datetime import datetime

from modules.db_connector import DBConnector
from modules.workload_analyzer import WorkloadAnalyzer
from modules.recommender import Recommender
from modules.evaluator import PerformanceEvaluator
from modules.explain_parser import ExplainParser
from modules.samples import get_samples, get_flat_workload
from modules.fingerprint import WorkloadFingerprinter

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ─── Absolute paths (works no matter what CWD Flask is started from) ──────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.yaml')

# Vercel filesystem fix: Use /tmp for SQLite DB
if os.environ.get('VERCEL'):
    DB_PATH = '/tmp/projects.db'
else:
    DB_PATH = os.path.join(BASE_DIR, 'projects.db')


# ─── Projects DB ──────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            host TEXT NOT NULL,
            port INTEGER NOT NULL,
            user TEXT NOT NULL,
            database TEXT NOT NULL,
            workload_path TEXT,
            last_accessed DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()


def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


def get_db_connector():
    dynamic_creds = session.get('db_credentials')
    if dynamic_creds:
        return DBConnector({'database': dynamic_creds})
    config = load_config()
    return DBConnector(config)


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route('/')
def home():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM projects ORDER BY last_accessed DESC')
    projects = c.fetchall()
    conn.close()
    return render_template('home.html', projects=projects)


@app.route('/dashboard')
def dashboard():
    db = get_db_connector()
    db_status = False
    db_info   = 'Not Connected'
    try:
        db.connect()
        db_status = True
        desc = session.get('db_credentials', load_config()['database'])
        db_info = f"{desc['user']}@{desc['host']}:{desc['port']}/{desc['database']}"
    except Exception:
        db_status = False
    finally:
        db.close()
    return render_template('index.html', db_status=db_status, db_info=db_info)


@app.route('/api/connect', methods=['POST'])
def test_connection():
    data = request.json
    creds = {
        'host':     data.get('host', 'localhost'),
        'port':     int(data.get('port', 3306)),
        'user':     data.get('user', 'root'),
        'password': data.get('password', ''),
        'database': data.get('database', 'demodb'),
    }
    db = DBConnector({'database': creds})
    try:
        db.connect()
        session['db_credentials'] = creds
        workload_path = data.get('workloadPath', 'queries.txt')
        session['workload_path']  = workload_path

        proj_name = data.get('projectName', f"{creds['database']} Project")
        conn = sqlite3.connect(DB_PATH)
        c    = conn.cursor()
        c.execute('SELECT id FROM projects WHERE host=? AND port=? AND database=?',
                  (creds['host'], creds['port'], creds['database']))
        existing = c.fetchone()
        if existing:
            c.execute('UPDATE projects SET last_accessed=CURRENT_TIMESTAMP, workload_path=? WHERE id=?',
                      (workload_path, existing[0]))
        else:
            c.execute(
                'INSERT INTO projects (name, host, port, user, database, workload_path) VALUES (?,?,?,?,?,?)',
                (proj_name, creds['host'], creds['port'], creds['user'], creds['database'], workload_path)
            )
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Successfully connected!'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    finally:
        db.close()


@app.route('/api/load_project/<int:project_id>')
def load_project(project_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c    = conn.cursor()
    c.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
    project = c.fetchone()
    if project:
        session['db_credentials'] = {
            'host':     project['host'],
            'port':     project['port'],
            'user':     project['user'],
            'password': '',
            'database': project['database'],
        }
        session['workload_path'] = project['workload_path']
        c.execute('UPDATE projects SET last_accessed=CURRENT_TIMESTAMP WHERE id=?', (project_id,))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})
    conn.close()
    return jsonify({'status': 'error', 'message': 'Project not found'}), 404


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    session.pop('db_credentials', None)
    return jsonify({'status': 'success'})


# ─── Test connection (explicit lightweight ping) ───────────────────────────────
@app.route('/api/test-connection', methods=['POST'])
def api_test_connection():
    data  = request.json or {}
    creds = {
        'host':     data.get('host', 'localhost'),
        'port':     int(data.get('port', 3306)),
        'user':     data.get('user', 'root'),
        'password': data.get('password', ''),
        'database': data.get('database', 'employees'),
        'engine':   data.get('engine', 'mysql'),
    }
    db = DBConnector({'database': creds})
    ok = db.test_connection()
    if ok:
        return jsonify({'status': 'success', 'message': f"Connected to {creds['database']} on {creds['host']}"})
    return jsonify({'status': 'error', 'message': 'Connection failed. Check credentials.'}), 400


# ─── Sample queries ────────────────────────────────────────────────────────────
@app.route('/api/sample-queries')
def api_sample_queries():
    """Return the benchmark query groups for the Employees database."""
    return jsonify({'status': 'success', 'groups': get_samples()})


# ─── Single-query EXPLAIN ──────────────────────────────────────────────────────
@app.route('/api/explain', methods=['POST'])
def api_explain():
    """
    Run EXPLAIN on a single user-supplied query against the current session DB.
    Returns ExplainResult as JSON.
    """
    data  = request.json or {}
    query = data.get('query', '').strip()
    if not query:
        return jsonify({'status': 'error', 'message': 'No query provided.'}), 400

    db = get_db_connector()
    try:
        db.connect()
        creds  = session.get('db_credentials', load_config()['database'])
        engine = creds.get('engine', 'mysql')
        rows   = db.get_explain_plan(query)
        result = ExplainParser.parse(rows, engine=engine)
        return jsonify({'status': 'success', 'explain': result.to_dict()})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        db.close()


# ─── Benchmark: stream a built-in sample workload ──────────────────────────────
@app.route('/api/benchmark/stream')
def api_benchmark_stream():
    """
    Write the selected sample group to a temp file, then stream analysis
    via SSE — same as /api/analyze/stream but with built-in queries.
    Query params: group_id (optional, default = all groups)
    """
    group_id = request.args.get('group_id')  # e.g. 'full_scan_salary'
    workload  = get_flat_workload(group_id)

    # Write to a temp file (auto-cleaned up after this request)
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.txt',
                                     delete=False, encoding='utf-8')
    tmp.write(workload)
    tmp.flush()
    tmp.close()
    tmp_path = tmp.name

    def generate():
        db = get_db_connector()
        try:
            db.connect()
            metadata         = db.get_schema_metadata()
            existing_indexes = db.get_indexes()
            engine           = session.get('db_credentials', {}).get('engine', 'mysql')

            # ── Per-query EXPLAIN while streaming ─────────────────────────────
            query_explain_cache = {}  # query_text -> ExplainResult.to_dict()

            analyzer  = WorkloadAnalyzer(metadata)
            collected = []
            progress_events = []

            def on_progress(done):
                progress_events.append(done)

            from modules.db_connector import DBConnector as _DC  # local import
            # stream file
            qgen = db.fetch_workload('file', tmp_path)

            fingerprinter = WorkloadFingerprinter()
            explain_seen  = {}   # fp_template -> ExplainResult dict

            from modules.fingerprint import fingerprint
            def counting_gen():
                for q in qgen:
                    if len(collected) < 1000: # Limit memory usage for evaluator sample
                        collected.append(q)
                    
                    try:
                        fp = fingerprint(q)
                        if fp not in explain_seen:
                            try:
                                rows = db.get_explain_plan(q)
                                result = ExplainParser.parse(rows, engine=engine)
                                exp_dict = result.to_dict()
                                explain_seen[fp] = exp_dict
                                query_explain_cache[q[:80]] = exp_dict
                                fingerprinter.record(q, exp_dict)
                            except Exception:
                                explain_seen[fp] = None
                                fingerprinter.record(q, None)
                        else:
                            fingerprinter.record(q, explain_seen[fp])
                    except Exception:
                        fingerprinter.record(q, None)
                    yield q

            workload_stats = analyzer.analyze(counting_gen(), progress_callback=on_progress)

            for done_count in progress_events:
                yield f"data: {json.dumps({'type': 'progress', 'done': done_count})}\n\n"

            yield f"data: {json.dumps({'type': 'progress_label', 'label': 'Timing recommendations (live benchmarking)…'})}\n\n"

            recommender     = Recommender(metadata, existing_indexes)
            recommendations = recommender.generate_recommendations(workload_stats)

            evaluator = PerformanceEvaluator(db)
            eval_recs = evaluator.evaluate(recommendations, collected)

            fp_summary    = fingerprinter.summary()
            fp_top_groups = fingerprinter.top_groups(n=20)

            result_payload = {
                'type':            'result',
                'stats':           _serialize_stats(workload_stats),
                'recommendations': eval_recs,
                'explain_cache':   query_explain_cache,
                'fingerprint': {
                    'summary':    fp_summary,
                    'top_groups': fp_top_groups,
                },
                'is_benchmark':    True,
            }
            yield f"data: {json.dumps(result_payload)}\n\n"
            yield 'data: {"type":"done"}\n\n'

        except Exception as e:
            logging.error(f"Benchmark SSE error: {e}", exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            db.close()
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':    'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


# ─── Standard (non-streaming) analyse – backward-compatible ───────────────────
@app.route('/api/analyze', methods=['POST'])
def run_analysis():
    data   = request.json or {}
    db     = get_db_connector()
    config = load_config()
    try:
        db.connect()
        metadata         = db.get_schema_metadata()
        existing_indexes = db.get_indexes()

        q_file = data.get('workloadPath',
                 session.get('workload_path',
                 config['system'].get('query_file', 'queries.txt')))
        session['workload_path'] = q_file

        # Resolve relative paths against BASE_DIR
        if not os.path.isabs(q_file):
            q_file = os.path.join(BASE_DIR, q_file)

        source  = config['system'].get('workload_source', 'file')
        queries = db.fetch_workload(source, q_file)

        # Materialise a capped list for the evaluator (evaluator needs random-access)
        query_list = list(queries)
        if not query_list:
            return jsonify({'status': 'error', 'message': 'No workload queries found.'}), 400

        analyzer      = WorkloadAnalyzer(metadata)
        workload_stats = analyzer.analyze(iter(query_list))

        serializable_stats = _serialize_stats(workload_stats)

        recommender      = Recommender(metadata, existing_indexes)
        recommendations  = recommender.generate_recommendations(workload_stats)

        evaluator = PerformanceEvaluator(db)
        eval_recs = evaluator.evaluate(recommendations, query_list)

        return jsonify({
            'status':          'success',
            'stats':           serializable_stats,
            'recommendations': eval_recs,
        })
    except Exception as e:
        logging.error(f"Error during analysis: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        db.close()


# ─── SSE streaming analyse endpoint ───────────────────────────────────────────
@app.route('/api/analyze/stream', methods=['GET'])
def run_analysis_stream():
    """
    Server-Sent Events endpoint.  The client connects and receives:
      data: {"type":"progress","done":1000}
      data: {"type":"progress","done":2000}
      ...
      data: {"type":"result","stats":{...},"recommendations":[...]}
      data: {"type":"done"}

    Query params:
      workloadPath – path to the workload file (overrides session)
    """
    data   = request.args
    config = load_config()

    q_file = (data.get('workloadPath')
              or session.get('workload_path')
              or config['system'].get('query_file', 'queries.txt'))
    session['workload_path'] = q_file

    if not os.path.isabs(q_file):
        q_file = os.path.join(BASE_DIR, q_file)

    source = config['system'].get('workload_source', 'file')

    def generate():
        db = get_db_connector()
        try:
            db.connect()
            metadata         = db.get_schema_metadata()
            existing_indexes = db.get_indexes()

            analyzer   = WorkloadAnalyzer(metadata)
            query_gen  = db.fetch_workload(source, q_file)

            progress_events = []

            def on_progress(done):
                progress_events.append(done)

            # We need to collect queries for the evaluator but still stream
            # progress – so we wrap the generator to accumulate + emit progress.
            collected = []

            def counting_gen():
                for q in query_gen:
                    if len(collected) < 1000: # Limit memory usage for evaluator sample
                        collected.append(q)
                    yield q

            # Run analysis; progress_callback fires every 1 000 queries
            workload_stats = analyzer.analyze(counting_gen(), progress_callback=on_progress)

            # Yield all accumulated progress events
            for done_count in progress_events:
                yield f"data: {json.dumps({'type': 'progress', 'done': done_count})}\n\n"

            # Build recommendations
            recommender     = Recommender(metadata, existing_indexes)
            recommendations = recommender.generate_recommendations(workload_stats)

            evaluator = PerformanceEvaluator(db)
            eval_recs = evaluator.evaluate(recommendations, collected)

            result_payload = {
                'type':            'result',
                'stats':           _serialize_stats(workload_stats),
                'recommendations': eval_recs,
            }
            yield f"data: {json.dumps(result_payload)}\n\n"
            yield "data: {\"type\":\"done\"}\n\n"

        except Exception as e:
            logging.error(f"SSE analysis error: {e}", exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            db.close()

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':   'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _serialize_stats(workload_stats):
    return {
        'where': [
            {'col': k, 'count': v}
            for k, v in sorted(workload_stats['where'].items(),   key=lambda i: i[1], reverse=True)
        ],
        'join': [
            {'col': k, 'count': v}
            for k, v in sorted(workload_stats['join'].items(),    key=lambda i: i[1], reverse=True)
        ],
        'groupby': [
            {'col': k, 'count': v}
            for k, v in sorted(workload_stats['groupby'].items(), key=lambda i: i[1], reverse=True)
        ],
        'anti_patterns':  workload_stats['anti_patterns'],
        'total_queries':  workload_stats['total_queries'],
    }


if __name__ == '__main__':
    app.run(debug=True, port=5000)
