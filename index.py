from flask import Flask, render_template, request, jsonify
import re
import os
import sqlite3

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

# Resolve absolute path to the SQLite database
# On Vercel, __file__ is in the same directory as data.db
# This works both locally and on Vercel serverless
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data.db')

# Fallback: check if DB exists, otherwise try current working directory
if not os.path.exists(DB_PATH):
    DB_PATH = os.path.join(os.getcwd(), 'data.db')

@app.route('/test', methods=['GET'])
def test():
    return jsonify({"message": "Hello, World!"}), 200

@app.route('/diagnostic', methods=['GET'])
def diagnostic():
    """Diagnostic endpoint to check database accessibility on Vercel"""
    return jsonify({
        "db_path": DB_PATH,
        "db_exists": os.path.exists(DB_PATH),
        "file_location": __file__,
        "working_directory": os.getcwd(),
        "directory_contents": os.listdir(os.path.dirname(os.path.abspath(__file__)))
    }), 200

@app.route('/county_data', methods=['GET', 'POST'])
def county_data():
    # Handle both GET (query params) and POST (JSON body)
    if request.method == 'POST':
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400
        
        payload = request.get_json(silent=True)
        if payload is None or not isinstance(payload, dict):
            return jsonify({"error": "Invalid JSON body"}), 400
    else:
        # GET request - use query parameters
        payload = request.args.to_dict()

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
        # Debug logging for Vercel
        print(f"DB_PATH: {DB_PATH}")
        print(f"DB exists: {os.path.exists(DB_PATH)}")
        print(f"__file__: {__file__}")
        print(f"cwd: {os.getcwd()}")
        
        if not os.path.exists(DB_PATH):
            return jsonify({"error": f"Database file not found at {DB_PATH}"}), 500
            
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
