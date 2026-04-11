"""
RFP Calendar — a minimal shared calendar for tracking customer RFPs.

Run locally:
    pip install -r requirements.txt
    python app.py

Then open http://localhost:5000 in a browser. To share with your sales team,
run this on a machine reachable on your LAN and have them hit
http://<that-host>:5000.

All data lives in rfp_calendar.db (SQLite) next to this file.
"""
import sqlite3
from pathlib import Path

from flask import Flask, g, jsonify, render_template, request

DB_PATH = Path(__file__).parent / "rfp_calendar.db"

app = Flask(__name__)


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_db(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


def init_db():
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS rfps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer TEXT NOT NULL,
                due_date TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/rfps", methods=["GET"])
def list_rfps():
    rows = get_db().execute(
        "SELECT id, customer, due_date FROM rfps ORDER BY due_date"
    ).fetchall()
    return jsonify(
        [
            {
                "id": row["id"],
                "title": row["customer"],
                "start": row["due_date"],
                "allDay": True,
            }
            for row in rows
        ]
    )


@app.route("/api/rfps", methods=["POST"])
def create_rfp():
    data = request.get_json(silent=True) or {}
    customer = (data.get("customer") or "").strip()
    due_date = (data.get("due_date") or "").strip()

    if not customer or not due_date:
        return jsonify({"error": "customer and due_date are required"}), 400

    db = get_db()
    cursor = db.execute(
        "INSERT INTO rfps (customer, due_date) VALUES (?, ?)",
        (customer, due_date),
    )
    db.commit()
    return (
        jsonify({"id": cursor.lastrowid, "customer": customer, "due_date": due_date}),
        201,
    )


@app.route("/api/rfps/<int:rfp_id>", methods=["DELETE"])
def delete_rfp(rfp_id):
    db = get_db()
    db.execute("DELETE FROM rfps WHERE id = ?", (rfp_id,))
    db.commit()
    return "", 204


if __name__ == "__main__":
    init_db()
    # debug=False on purpose: Flask's debug mode exposes an interactive
    # console that is unsafe to expose on a shared network.
    app.run(host="0.0.0.0", port=5000, debug=False)
