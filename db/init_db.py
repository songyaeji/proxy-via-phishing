# db/init_db.py
import sqlite3
from pathlib import Path

URLS_COLUMNS = {
    "source": "TEXT NOT NULL",
    "proxy_type": "TEXT",
    "task_url": "TEXT NOT NULL",
    "page_url": "TEXT",
    "second_page_url": "TEXT",
    "final_redirect_url": "TEXT",
    "script_redirect_url": "TEXT",
    "redirect_snippet": "TEXT",
    "base_domain": "TEXT",
    "verdict": "TEXT",
    "score": "REAL",
    "malicious": "BOOLEAN",
    "country": "TEXT",
    "ip": "TEXT",
    "http_requests": "INTEGER",
    "unique_ips": "INTEGER",
    "urlscan_timestamp": "TEXT",
    "collected_at": "TEXT",
    "status_checked": "BOOLEAN DEFAULT 0",
    "last_status_code": "INTEGER",
    "notes": "TEXT",
}

def _table_exists(cur, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table});")
    return column in [row[1] for row in cur.fetchall()]

def ensure_urls_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    if not _table_exists(cur, "urls"):
        cur.execute("""
        CREATE TABLE urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            proxy_type TEXT,
            task_url TEXT NOT NULL,
            page_url TEXT,
            second_page_url TEXT,
            final_redirect_url TEXT,
            script_redirect_url TEXT,
            redirect_snippet TEXT,
            base_domain TEXT,
            verdict TEXT,
            score REAL,
            malicious BOOLEAN,
            country TEXT,
            ip TEXT,
            http_requests INTEGER,
            unique_ips INTEGER,
            urlscan_timestamp TEXT,
            collected_at TEXT,
            status_checked BOOLEAN DEFAULT 0,
            last_status_code INTEGER,
            notes TEXT
        );
        """)
        return

    # 있으면 부족한 컬럼 추가
    for col, coltype in URLS_COLUMNS.items():
        if not _column_exists(cur, "urls", col):
            cur.execute(f"ALTER TABLE urls ADD COLUMN {col} {coltype};")

def init_db(db_path: str):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        ensure_urls_table(conn)
        conn.commit()
    finally:
        conn.close()
    print(f"[init_db] Initialized {db_path}")
