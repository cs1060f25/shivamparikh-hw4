from flask import Flask, render_template, request, jsonify
import re
import os
import sqlite3
import subprocess
import sys

app = Flask(__name__)



@app.route('/')
def index():
    return render_template('index.html')

ALLOWED_MEASURE_NAMES = [
    "Violent crime rate",
    "Unemployment",
    "Children in poverty",
    "Diabetic screening",
    "Mammography screening",
    "Preventable hospital stays",
    "Uninsured",
    "Sexually transmitted infections",
    "Physical inactivity",
    "Adult obesity",
    "Premature Death",
    "Daily fine particulate matter",
]

def _normalize_measure_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().lower())

ALLOWED_MEASURE_CANONICAL = { _normalize_measure_name(n): n for n in ALLOWED_MEASURE_NAMES }

"""Database bootstrap on cold start.
We try to create data.db using csv_to_sqlite.py if it doesn't exist.
In read-only serverless environments, we fall back to /tmp which is writable.
"""

_BASE_DIR = os.path.abspath(os.path.dirname(__file__))
_ROOT_DIR = os.path.abspath(os.path.join(_BASE_DIR))  # file sits at project root alongside CSVs

_SCRIPT = os.path.join(_ROOT_DIR, 'csv_to_sqlite.py')
_CSV1 = os.path.join(_ROOT_DIR, 'county_health_rankings.csv')
_CSV2 = os.path.join(_ROOT_DIR, 'zip_county.csv')

_PRIMARY_DB = os.path.join(_ROOT_DIR, 'data.db')
_TMP_DB = os.path.join('/tmp', 'data.db')

def _file_exists(path: str) -> bool:
    try:
        return os.path.isfile(path)
    except Exception:
        return False

def _try_bootstrap_db(target_db: str) -> None:
    if not _file_exists(_SCRIPT) or not _file_exists(_CSV1) or not _file_exists(_CSV2):
        return
    # Run imports sequentially; avoid interactive prompts and capture errors for logging
    cmds = [
        [sys.executable, _SCRIPT, target_db, _CSV1],
        [sys.executable, _SCRIPT, target_db, _CSV2],
    ]
    for cmd in cmds:
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception:
            # If it fails (e.g., read-only FS), just stop trying
            break

# Decide target DB path
DB_PATH = _PRIMARY_DB
if not _file_exists(DB_PATH):
    # Try to create at root; if fails, try /tmp
    try:
        _try_bootstrap_db(DB_PATH)
    except Exception:
        pass
if not _file_exists(DB_PATH):
    try:
        _try_bootstrap_db(_TMP_DB)
        if _file_exists(_TMP_DB):
            DB_PATH = _TMP_DB
    except Exception:
        pass

@app.route('/test', methods=['GET'])
def test():
    return jsonify({"message": "Hello, World!"}), 200

@app.route('/county_data', methods=['POST'])
def county_data():
    # Require JSON body
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    payload = request.get_json(silent=True)
    if payload is None or not isinstance(payload, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    zip_value = payload.get('zip')
    measure_name = payload.get('measure_name')
    coffee = payload.get('coffee')
    if(coffee == "teapot"):
        return jsonify({"error": "I'm a teapot"}), 418

    # Validate presence
    if zip_value is None or measure_name is None:
        return jsonify({"error": "zip and measure_name are required"}), 400

    # Validate zip format: five digit number
    zip_str = str(zip_value)
    if not re.fullmatch(r"\d{5}", zip_str):
        return jsonify({"error": "zip must be a five digit number"}), 400

    # Validate measure name against allowed list (case-insensitive, canonicalized)
    canonical_measure = ALLOWED_MEASURE_CANONICAL.get(_normalize_measure_name(measure_name))
    if not canonical_measure:
        return jsonify({"error": "Invalid measure_name"}), 400

    # Query database with parameterized SQL to prevent injection
    try:
        print(DB_PATH)
        conn = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
        conn.row_factory = sqlite3.Row
        sql = (
            """
            SELECT c.*
            FROM county_health_rankings AS c
            JOIN zip_county AS z
              ON c.state = z.state_abbreviation
             AND c.county = z.county
            WHERE z.zip = ?
              AND c.measure_name = ?
            """
        )
        cur = conn.execute(sql, (zip_str, canonical_measure))
        rows = cur.fetchall()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Database error"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not rows:
        return jsonify({"error": "Not found"}), 404

    # Return all columns from county_health_rankings as JSON
    return jsonify([dict(row) for row in rows]), 200

@app.errorhandler(404)
def handle_404(_e):
    if request.is_json or 'application/json' in (request.headers.get('Accept') or ''):
        return jsonify({"error": "Not found"}), 404
    return "Not Found", 404


if __name__ == '__main__':
    app.run(debug=True)
