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
import calendar as cal
import os
import sqlite3
from datetime import date
from pathlib import Path

from flask import Flask, g, jsonify, redirect, render_template, request, session, url_for

DB_PATH = Path(__file__).parent / "rfp_calendar.db"

# ── Change this to your team's password ──
TEAM_PASSWORD = os.environ.get("RFP_PASSWORD", "rfp2026")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "replace-me-with-something-random")


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
                added_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # Migrate existing databases that don't have the added_by column yet
        cols = [r[1] for r in db.execute("PRAGMA table_info(rfps)").fetchall()]
        if "added_by" not in cols:
            db.execute("ALTER TABLE rfps ADD COLUMN added_by TEXT NOT NULL DEFAULT ''")


@app.before_request
def require_login():
    open_endpoints = ("login", "static")
    if request.endpoint in open_endpoints:
        return
    if not session.get("authenticated"):
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == TEAM_PASSWORD:
            session["authenticated"] = True
            return redirect(url_for("index"))
        error = "Wrong password"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/rfps", methods=["GET"])
def list_rfps():
    rows = get_db().execute(
        "SELECT id, customer, due_date, added_by FROM rfps ORDER BY due_date"
    ).fetchall()
    return jsonify(
        [
            {
                "id": row["id"],
                "title": f"{row['customer']} ({row['added_by']})" if row["added_by"] else row["customer"],
                "start": row["due_date"],
                "allDay": True,
                "extendedProps": {
                    "customer": row["customer"],
                    "added_by": row["added_by"],
                },
            }
            for row in rows
        ]
    )


@app.route("/api/rfps", methods=["POST"])
def create_rfp():
    data = request.get_json(silent=True) or {}
    customer = (data.get("customer") or "").strip()
    due_date = (data.get("due_date") or "").strip()
    added_by = (data.get("added_by") or "").strip()

    if not customer or not due_date or not added_by:
        return jsonify({"error": "customer, due_date, and added_by are required"}), 400

    db = get_db()
    cursor = db.execute(
        "INSERT INTO rfps (customer, due_date, added_by) VALUES (?, ?, ?)",
        (customer, due_date, added_by),
    )
    db.commit()
    return (
        jsonify({"id": cursor.lastrowid, "customer": customer, "due_date": due_date, "added_by": added_by}),
        201,
    )


@app.route("/api/summary", methods=["GET"])
def summary():
    year = request.args.get("year", date.today().year, type=int)
    db = get_db()
    rows = db.execute(
        "SELECT id, customer, due_date, added_by FROM rfps "
        "WHERE due_date >= ? AND due_date < ? ORDER BY due_date",
        (f"{year}-01-01", f"{year + 1}-01-01"),
    ).fetchall()

    months = []
    for m in range(1, 13):
        prefix = f"{year}-{m:02d}"
        month_rfps = [
            {"id": r["id"], "customer": r["customer"], "due_date": r["due_date"], "added_by": r["added_by"]}
            for r in rows
            if r["due_date"].startswith(prefix)
        ]
        months.append({
            "month": m,
            "name": cal.month_name[m],
            "count": len(month_rfps),
            "rfps": month_rfps,
        })

    return jsonify({
        "year": year,
        "total": len(rows),
        "months": months,
    })


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
