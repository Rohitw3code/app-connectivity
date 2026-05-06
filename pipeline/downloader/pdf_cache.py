from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Iterable

_START_DIR = Path(__file__).resolve().parent.parent.parent
_DEFAULT_DB_NAME = "pipeline_tracker.db"
_TABLE_NAME = "scraped_pdfs"
_CACHE_INSTANCES: dict[tuple[str | None, str, str], "PdfCache"] = {}


def _sanitize_key(key: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", key.strip())
    cleaned = cleaned.lstrip("0123456789_") or "source"
    return cleaned.lower()


def _quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _default_db_path() -> Path:
    return _START_DIR / _DEFAULT_DB_NAME


class PdfCache:
    def __init__(self, db_path: str | Path | None, source_key: str, source_name: str | None = None) -> None:
        self._db_path = Path(db_path) if db_path else _default_db_path()
        self._source_key = _sanitize_key(source_key)
        self._source_name = (source_name or source_key).strip() or source_key
        self._conn: sqlite3.Connection | None = None

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self._db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=DELETE")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._ensure_table()
        return self._conn

    def _ensure_table(self) -> None:
        conn = self._conn
        if conn is None:
            return
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scraped_pdfs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL,
                source_name TEXT NOT NULL DEFAULT '',
                pdf_name TEXT NOT NULL,
                pdf_type TEXT NOT NULL DEFAULT '',
                pdf_path TEXT NOT NULL DEFAULT '',
                extraction_done INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_name, pdf_name, pdf_type)
            )
            """
        )
        self._ensure_columns(conn, _TABLE_NAME, {
            "source_name": "TEXT NOT NULL DEFAULT ''",
            "pdf_type": "TEXT NOT NULL DEFAULT ''",
            "pdf_path": "TEXT NOT NULL DEFAULT ''",
            "extraction_done": "INTEGER NOT NULL DEFAULT 0",
        })
        conn.execute(
            "CREATE INDEX IF NOT EXISTS scraped_pdfs_source_idx ON scraped_pdfs (source_name)"
        )
        self._ensure_source_table(conn)
        self._migrate_source_table(conn)
        self._seed_source_table_from_generic(conn)
        conn.commit()

    def _ensure_columns(self, conn: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}  # type: ignore[index]
        for column, definition in columns.items():
            if column in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {definition}")

    def _ensure_source_table(self, conn: sqlite3.Connection) -> None:
        table = self._quoted_source_table()
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pdf_name TEXT NOT NULL,
                pdf_type TEXT NOT NULL DEFAULT '',
                pdf_path TEXT NOT NULL DEFAULT '',
                extraction_done INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(pdf_name, pdf_type)
            )
            """
        )
        self._ensure_columns(conn, table, {
            "extraction_done": "INTEGER NOT NULL DEFAULT 0",
        })
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self._source_index_name()} ON {table} (pdf_name)"
        )

    def _quoted_source_table(self) -> str:
        return _quote_identifier(self._source_name)

    def _source_index_name(self) -> str:
        safe = _sanitize_key(self._source_name)
        return _quote_identifier(f"{safe}_pdf_name_idx")

    def _migrate_source_table(self, conn: sqlite3.Connection) -> None:
        legacy_table = f"source_{self._source_key}"
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (legacy_table,),
        ).fetchone()
        if row is None:
            return

        conn.execute(
            f"""
            INSERT OR IGNORE INTO {_TABLE_NAME} (source_key, source_name, pdf_name, pdf_type, pdf_path)
            SELECT ?, ?, pdf_name, '', '' FROM {legacy_table}
            """,
            (self._source_key, self._source_name),
        )
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {self._quoted_source_table()} (pdf_name, pdf_type, pdf_path)
            SELECT pdf_name, '', '' FROM {legacy_table}
            """
        )

    def _seed_source_table_from_generic(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {self._quoted_source_table()} (pdf_name, pdf_type, pdf_path, extraction_done)
            SELECT pdf_name, pdf_type, pdf_path, extraction_done FROM {_TABLE_NAME}
            WHERE source_name = ?
            """,
            (self._source_name,),
        )

    def _resolve_pdf_type(self, pdf_type: str | None, pdf_path: str | Path | None) -> str:
        if pdf_type is not None and str(pdf_type).strip():
            return str(pdf_type).strip()
        if pdf_path:
            path = Path(pdf_path).resolve()
            parts = path.parts
            if self._source_name in parts:
                idx = parts.index(self._source_name)
                if idx + 1 < len(parts) - 1:
                    return parts[idx + 1]
            parent = path.parent.name
            if parent and parent != self._source_name:
                return parent
        return ""

    def is_cached(self, pdf_name: str, pdf_type: str | None = None, pdf_path: str | Path | None = None) -> bool:
        conn = self._connect()
        resolved_type = self._resolve_pdf_type(pdf_type, pdf_path)
        if resolved_type:
            row = conn.execute(
                f"SELECT 1 FROM {self._quoted_source_table()} WHERE pdf_name = ? AND pdf_type = ? LIMIT 1",
                (pdf_name, resolved_type),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT 1 FROM {self._quoted_source_table()} WHERE pdf_name = ? LIMIT 1",
                (pdf_name,),
            ).fetchone()
        return row is not None

    def record_download(
        self,
        pdf_name: str,
        pdf_type: str | None = None,
        pdf_path: str | Path | None = None,
    ) -> None:
        conn = self._connect()
        resolved_type = self._resolve_pdf_type(pdf_type, pdf_path)
        resolved_path = str(pdf_path) if pdf_path else ""
        conn.execute(
            """
            INSERT OR IGNORE INTO scraped_pdfs (source_key, source_name, pdf_name, pdf_type, pdf_path)
            VALUES (?, ?, ?, ?, ?)
            """,
            (self._source_key, self._source_name, pdf_name, resolved_type, resolved_path),
        )
        conn.execute(
            f"""
            INSERT OR IGNORE INTO {self._quoted_source_table()} (pdf_name, pdf_type, pdf_path)
            VALUES (?, ?, ?)
            """,
            (pdf_name, resolved_type, resolved_path),
        )
        conn.commit()

    def record_existing_pdfs(self, pdf_names: Iterable[str | tuple[str, str] | tuple[str, str, str]]) -> int:
        entries: list[tuple[str, str, str]] = []
        for item in pdf_names:
            if isinstance(item, tuple):
                name = item[0]
                pdf_type = item[1] if len(item) > 1 else ""
                pdf_path = item[2] if len(item) > 2 else ""
            else:
                name = item
                pdf_type = ""
                pdf_path = ""
            if not name:
                continue
            entries.append((name, pdf_type, pdf_path))
        if not entries:
            return 0

        conn = self._connect()
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO scraped_pdfs (source_key, source_name, pdf_name, pdf_type, pdf_path, extraction_done)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            [(self._source_key, self._source_name, name, pdf_type, pdf_path) for name, pdf_type, pdf_path in entries],
        )
        conn.executemany(
            f"""
            INSERT OR IGNORE INTO {self._quoted_source_table()} (pdf_name, pdf_type, pdf_path, extraction_done)
            VALUES (?, ?, ?, 0)
            """,
            entries,
        )
        conn.commit()
        return conn.total_changes - before

    def mark_extracted(
        self,
        pdf_name: str,
        pdf_type: str | None = None,
        pdf_path: str | Path | None = None,
    ) -> None:
        conn = self._connect()
        resolved_type = self._resolve_pdf_type(pdf_type, pdf_path)
        resolved_path = str(pdf_path) if pdf_path else ""

        if resolved_type:
            conn.execute(
                f"""
                UPDATE {self._quoted_source_table()}
                SET extraction_done = 1, pdf_path = COALESCE(NULLIF(?, ''), pdf_path)
                WHERE pdf_name = ? AND pdf_type = ?
                """,
                (resolved_path, pdf_name, resolved_type),
            )
            conn.execute(
                f"""
                UPDATE {_TABLE_NAME}
                SET extraction_done = 1, pdf_path = COALESCE(NULLIF(?, ''), pdf_path)
                WHERE source_name = ? AND pdf_name = ? AND pdf_type = ?
                """,
                (resolved_path, self._source_name, pdf_name, resolved_type),
            )
        else:
            conn.execute(
                f"""
                UPDATE {self._quoted_source_table()}
                SET extraction_done = 1, pdf_path = COALESCE(NULLIF(?, ''), pdf_path)
                WHERE pdf_name = ?
                """,
                (resolved_path, pdf_name),
            )
            conn.execute(
                f"""
                UPDATE {_TABLE_NAME}
                SET extraction_done = 1, pdf_path = COALESCE(NULLIF(?, ''), pdf_path)
                WHERE source_name = ? AND pdf_name = ?
                """,
                (resolved_path, self._source_name, pdf_name),
            )
        conn.commit()

    def get_pending_extractions(self) -> list[dict]:
        conn = self._connect()
        rows = conn.execute(
            f"""
            SELECT pdf_name, pdf_type, pdf_path, extraction_done
            FROM {self._quoted_source_table()}
            WHERE extraction_done = 0
            ORDER BY pdf_name
            """,
        ).fetchall()
        return [dict(r) for r in rows]


def get_pdf_cache(db_path: str | Path | None, source_key: str, source_name: str | None = None) -> PdfCache:
    key = (str(db_path) if db_path else None, source_key, source_name or source_key)
    cache = _CACHE_INSTANCES.get(key)
    if cache is None:
        cache = PdfCache(db_path, source_key, source_name)
        _CACHE_INSTANCES[key] = cache
    return cache
