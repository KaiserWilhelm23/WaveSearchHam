import os
import sys
import json
import socket
import argparse
import sqlite3
import subprocess
import platform
from http.server import SimpleHTTPRequestHandler
import socketserver


# =========================
# GLOBAL SQLITE DB
# =========================
AMATEUR_SQL_CONN = None


def load_amateur_sql(sql_path):
    global AMATEUR_SQL_CONN

    if not os.path.exists(sql_path):
        print(f"Missing SQL file: {sql_path}")
        return

    try:
        AMATEUR_SQL_CONN = sqlite3.connect(":memory:", check_same_thread=False)

        with open(sql_path, "r", encoding="utf-8") as f:
            AMATEUR_SQL_CONN.executescript(f.read())

        print("Loaded amateur_calls database into memory")

    except Exception as e:
        print(f"SQL load failed: {e}")
        AMATEUR_SQL_CONN = None


# =========================
# NETWORK HELPERS
# =========================
def get_lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"


# =========================
# HTTP HANDLER
# =========================
class AmateurHandler(SimpleHTTPRequestHandler):

    # 🔥 KEY FIX: serve from /sites instead of script folder
    def translate_path(self, path):
        base_dir = os.path.join(os.path.dirname(__file__), "sites")
        path = path.split("?", 1)[0].split("#", 1)[0]
        path = path.lstrip("/")
        return os.path.join(base_dir, path)

    def do_POST(self):
        if self.path != "/search":
            return self.send_error(404, "Not found")

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            query = json.loads(body)
        except:
            return self._json({"error": "invalid json"}, 400)

        if query.get("dataset") != "amateur_calls":
            return self._json({"error": "invalid dataset"}, 400)

        if AMATEUR_SQL_CONN is None:
            return self._json({"error": "database not loaded"}, 500)

        sql = """
        SELECT 
            a.call_sign, a.name, a.street,
            c.city, s.state, z.zip, a.frn
        FROM amateur_calls a
        LEFT JOIN cities c ON a.city_id = c.id
        LEFT JOIN states s ON a.state_id = s.id
        LEFT JOIN zips z ON a.zip_id = z.id
        WHERE 1=1
        """

        params = []

        for field, val in query.items():
            if field == "dataset":
                continue

            if field in ["call_sign", "name", "street", "frn"]:
                sql += f" AND a.{field} LIKE ?"
                params.append(f"%{val}%")

            elif field == "city":
                sql += " AND c.city LIKE ?"
                params.append(f"%{val}%")

            elif field == "state":
                sql += " AND s.state LIKE ?"
                params.append(f"%{val}%")

            elif field == "zip":
                if isinstance(val, list):
                    zips = val
                elif isinstance(val, str):
                    zips = [z.strip() for z in val.split(",") if z.strip()]
                else:
                    zips = [str(val)]

                if zips:
                    placeholders = ",".join(["?"] * len(zips))
                    sql += f" AND z.zip IN ({placeholders})"
                    params.extend(zips)

        try:
            cur = AMATEUR_SQL_CONN.cursor()
            cur.execute(sql, params)

            cols = [c[0] for c in cur.description]
            results = [dict(zip(cols, row)) for row in cur.fetchall()]

            return self._json(results)

        except Exception as e:
            return self._json({"error": str(e)}, 500)

    def _json(self, data, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))


# =========================
# SERVER START
# =========================
def run_server(port, sql_path):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    load_amateur_sql(sql_path)

    host = "0.0.0.0"
    lan_ip = get_lan_ip()

    print("\nServer running:")
    print(f"  Local: http://localhost:{port}/search.html")
    print(f"  LAN:   http://{lan_ip}:{port}/search.html")
    print("\nServing from /sites folder\n")

    with socketserver.ThreadingTCPServer((host, port), AmateurHandler) as httpd:
        httpd.serve_forever()


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--sql", type=str, default="amateur_calls.sql")
    args = parser.parse_args()

    run_server(args.port, args.sql)