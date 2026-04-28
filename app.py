"""
app.py — Grant Monitor Desktop Application
Launch: python app.py
"""

import threading
import json
import os
import sys
import webbrowser
from flask import Flask, render_template, request, jsonify

# Ensure we can find our modules
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
sys.path.insert(0, BASE_DIR)

import grant_engine as engine

engine.init_db()

# ==============================================================
# FLASK APP
# ==============================================================

flask_app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
)
flask_app.config["SECRET_KEY"] = os.urandom(24)


@flask_app.route("/")
def index():
    return render_template("index.html")


# -- API: Keywords --

@flask_app.route("/api/keywords", methods=["GET"])
def api_get_keywords():
    return jsonify({"keywords": engine.get_active_keywords()})


@flask_app.route("/api/keywords", methods=["POST"])
def api_add_keyword():
    data = request.get_json()
    kw = data.get("keyword", "").strip()
    if not kw:
        return jsonify({"status": "error", "message": "Keyword cannot be empty."})
    engine.add_keyword(kw)
    return jsonify({"status": "ok", "message": f"Added: {kw}"})


@flask_app.route("/api/keywords", methods=["DELETE"])
def api_remove_keyword():
    data = request.get_json()
    kw = data.get("keyword", "").strip()
    if not kw:
        return jsonify({"status": "error", "message": "Keyword cannot be empty."})
    engine.remove_keyword(kw)
    return jsonify({"status": "ok", "message": f"Removed: {kw}"})


# -- API: Scan --

@flask_app.route("/api/scan", methods=["POST"])
def api_scan():
    try:
        summary = engine.run_full_scan()
        with engine.get_db() as conn:
            conn.execute("UPDATE grants SET notified = 1 WHERE notified = 0")
        return jsonify({"status": "ok", "summary": summary})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# -- API: Get grants --

@flask_app.route("/api/grants", methods=["GET"])
def api_get_grants():
    days = int(request.args.get("days", 7))
    source = request.args.get("source", "all")
    df = engine.get_grants_df(since_days=days, source_filter=source)
    grants = []
    for _, row in df.iterrows():
        grants.append({
            "title": row["title"],
            "url": row["url"],
            "source": row["source"],
            "description": row.get("description", ""),
            "deadline": row.get("deadline", ""),
            "posted_date": row.get("posted_date", ""),
            "funding_amount": row.get("funding_amount", ""),
            "matched_keywords": row.get("matched_keywords", ""),
            "first_seen": row.get("first_seen", ""),
            "status": row.get("status", "NEW"),
        })
    return jsonify({"grants": grants, "count": len(grants)})


# -- API: Exports --

@flask_app.route("/api/export/excel", methods=["POST"])
def api_export_excel():
    try:
        filename = engine.generate_grant_excel()
        if filename:
            return jsonify({"status": "ok", "filename": filename})
        else:
            return jsonify({"status": "error", "message": "No grants in database."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@flask_app.route("/api/export/csv", methods=["POST"])
def api_export_csv():
    try:
        days = int(request.args.get("days", 7))
        filename = engine.export_csv(since_days=days)
        return jsonify({"status": "ok", "filename": filename})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


# ==============================================================
# LAUNCH
# ==============================================================

def main():
    USE_DESKTOP_WINDOW = True

    if USE_DESKTOP_WINDOW:
        try:
            import webview

            def start_flask():
                flask_app.run(host="127.0.0.1", port=5199,
                              debug=False, use_reloader=False)

            server = threading.Thread(target=start_flask, daemon=True)
            server.start()

            import time
            time.sleep(1.5)

            webview.create_window(
                title="Grant Monitor",
                url="http://127.0.0.1:5199",
                width=1200, height=850, min_size=(900, 600),
            )
            webview.start(debug=False)

        except ImportError:
            print("pywebview not found — falling back to browser mode")
            webbrowser.open("http://127.0.0.1:5199")
            flask_app.run(host="127.0.0.1", port=5199, debug=False)
    else:
        webbrowser.open("http://127.0.0.1:5199")
        flask_app.run(host="127.0.0.1", port=5199, debug=False)


if __name__ == "__main__":
    main()
