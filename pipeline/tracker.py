"""
pipeline/tracker.py — SQLite Pipeline Tracker
===============================================
Central tracking database for downloads, extractions, excel generation,
and extracted row records.  Supports resume from any interruption point.

Usage:
    from pipeline.tracker import PipelineTracker
    tracker = PipelineTracker()            # opens/creates pipeline_tracker.db
    tracker = PipelineTracker("custom.db") # custom path
"""

from __future__ import annotations

import json
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_START_DIR = Path(__file__).resolve().parent.parent  # …/start/
DEFAULT_DB_PATH = _START_DIR / "pipeline_tracker.db"


class PipelineTracker:
    """Thin wrapper around a SQLite DB that tracks pipeline state."""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path).resolve() if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()
        logger.info("[Tracker] DB → %s", self.db_path)

    # ─── Schema ───────────────────────────────────────────────────────────────

    def _create_tables(self) -> None:
        cur = self.conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                handler         TEXT    NOT NULL,
                pdf_filename    TEXT    NOT NULL,
                pdf_path        TEXT    NOT NULL,
                source_url      TEXT    DEFAULT '',
                region          TEXT    DEFAULT '',
                doc_type        TEXT    DEFAULT '',
                downloaded_at   TEXT    NOT NULL,
                file_size_bytes INTEGER DEFAULT 0
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS extractions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                download_id     INTEGER,
                handler         TEXT    NOT NULL,
                pdf_filename    TEXT    NOT NULL,
                status          TEXT    NOT NULL DEFAULT 'pending',
                rows_extracted  INTEGER DEFAULT 0,
                json_cache_path TEXT    DEFAULT '',
                started_at      TEXT,
                completed_at    TEXT,
                error_message   TEXT    DEFAULT '',
                FOREIGN KEY (download_id) REFERENCES downloads(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS excels (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                handler         TEXT    NOT NULL,
                excel_path      TEXT    NOT NULL,
                sheet_name      TEXT    DEFAULT '',
                row_count       INTEGER DEFAULT 0,
                generated_at    TEXT    NOT NULL,
                status          TEXT    NOT NULL DEFAULT 'generated'
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at       TEXT    NOT NULL,
                completed_at     TEXT,
                download_limit   INTEGER DEFAULT 5,
                total_downloaded INTEGER DEFAULT 0,
                total_extracted  INTEGER DEFAULT 0,
                total_excels     INTEGER DEFAULT 0,
                status           TEXT    NOT NULL DEFAULT 'running'
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS records (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                handler         TEXT    NOT NULL,
                gna_id          TEXT    DEFAULT '',
                lta_id          TEXT    DEFAULT '',
                unique_key      TEXT    UNIQUE,
                pdf_filename    TEXT    DEFAULT '',
                page_number     INTEGER DEFAULT 0,
                data_json       TEXT    DEFAULT '{}',
                created_at      TEXT    NOT NULL
            )
        """)

        self.conn.commit()

    # ─── Pipeline Runs ────────────────────────────────────────────────────────

    def start_run(self, download_limit: int = 5) -> int:
        """Register a new pipeline run. Returns the run_id."""
        now = datetime.now().isoformat(timespec="seconds")
        cur = self.conn.execute(
            "INSERT INTO pipeline_runs (started_at, download_limit, status) VALUES (?, ?, 'running')",
            (now, download_limit),
        )
        self.conn.commit()
        return cur.lastrowid

    def finish_run(self, run_id: int, total_downloaded: int, total_extracted: int, total_excels: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "UPDATE pipeline_runs SET completed_at=?, total_downloaded=?, total_extracted=?, total_excels=?, status='completed' WHERE id=?",
            (now, total_downloaded, total_extracted, total_excels, run_id),
        )
        self.conn.commit()

    def fail_run(self, run_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "UPDATE pipeline_runs SET completed_at=?, status='failed' WHERE id=?",
            (now, run_id),
        )
        self.conn.commit()

    # ─── Downloads ────────────────────────────────────────────────────────────

    def register_download(
        self,
        handler: str,
        pdf_filename: str,
        pdf_path: str,
        source_url: str = "",
        region: str = "",
        doc_type: str = "",
        file_size_bytes: int = 0,
    ) -> int:
        """Register a downloaded PDF. Returns the download_id."""
        now = datetime.now().isoformat(timespec="seconds")
        cur = self.conn.execute(
            """INSERT INTO downloads
               (handler, pdf_filename, pdf_path, source_url, region, doc_type, downloaded_at, file_size_bytes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (handler, pdf_filename, pdf_path, source_url, region, doc_type, now, file_size_bytes),
        )
        self.conn.commit()
        return cur.lastrowid

    def is_downloaded(self, handler: str, pdf_filename: str) -> bool:
        """Check if a PDF was already downloaded."""
        row = self.conn.execute(
            "SELECT id FROM downloads WHERE handler=? AND pdf_filename=?",
            (handler, pdf_filename),
        ).fetchone()
        return row is not None

    def get_download_id(self, handler: str, pdf_filename: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT id FROM downloads WHERE handler=? AND pdf_filename=?",
            (handler, pdf_filename),
        ).fetchone()
        return row["id"] if row else None

    def count_downloads(self, handler: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM downloads WHERE handler=?",
            (handler,),
        ).fetchone()
        return row["cnt"]

    def get_downloads(self, handler: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM downloads WHERE handler=? ORDER BY id",
            (handler,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── Extractions ──────────────────────────────────────────────────────────

    def register_extraction(self, handler: str, pdf_filename: str, download_id: int | None = None) -> int:
        """Register a pending extraction. Returns the extraction_id."""
        cur = self.conn.execute(
            "INSERT INTO extractions (download_id, handler, pdf_filename, status) VALUES (?, ?, ?, 'pending')",
            (download_id, handler, pdf_filename),
        )
        self.conn.commit()
        return cur.lastrowid

    def start_extraction(self, extraction_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "UPDATE extractions SET status='in_progress', started_at=? WHERE id=?",
            (now, extraction_id),
        )
        self.conn.commit()

    def complete_extraction(self, extraction_id: int, rows_extracted: int, json_cache_path: str = "") -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "UPDATE extractions SET status='completed', rows_extracted=?, json_cache_path=?, completed_at=? WHERE id=?",
            (rows_extracted, json_cache_path, now, extraction_id),
        )
        self.conn.commit()

    def fail_extraction(self, extraction_id: int, error_message: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "UPDATE extractions SET status='failed', error_message=?, completed_at=? WHERE id=?",
            (error_message, now, extraction_id),
        )
        self.conn.commit()

    def is_extracted(self, handler: str, pdf_filename: str) -> bool:
        """Check if extraction is already completed for a PDF."""
        row = self.conn.execute(
            "SELECT id FROM extractions WHERE handler=? AND pdf_filename=? AND status='completed'",
            (handler, pdf_filename),
        ).fetchone()
        return row is not None

    def get_pending_extractions(self, handler: str) -> list[dict]:
        """Get PDFs that need extraction (pending or failed)."""
        rows = self.conn.execute(
            "SELECT * FROM extractions WHERE handler=? AND status IN ('pending', 'failed') ORDER BY id",
            (handler,),
        ).fetchall()
        return [dict(r) for r in rows]

    def count_extractions(self, handler: str, status: str = "completed") -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM extractions WHERE handler=? AND status=?",
            (handler, status),
        ).fetchone()
        return row["cnt"]

    # ─── Excels ───────────────────────────────────────────────────────────────

    def register_excel(self, handler: str, excel_path: str, sheet_name: str = "", row_count: int = 0) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        # Upsert: if same handler+path exists, update it
        existing = self.conn.execute(
            "SELECT id FROM excels WHERE handler=? AND excel_path=?",
            (handler, excel_path),
        ).fetchone()
        if existing:
            self.conn.execute(
                "UPDATE excels SET row_count=?, generated_at=?, status='regenerated', sheet_name=? WHERE id=?",
                (row_count, now, sheet_name, existing["id"]),
            )
            self.conn.commit()
            return existing["id"]
        else:
            cur = self.conn.execute(
                "INSERT INTO excels (handler, excel_path, sheet_name, row_count, generated_at, status) VALUES (?, ?, ?, ?, ?, 'generated')",
                (handler, excel_path, sheet_name, row_count, now),
            )
            self.conn.commit()
            return cur.lastrowid

    def is_excel_present(self, handler: str, excel_path: str) -> bool:
        """Check if the Excel file exists on disk AND is tracked."""
        row = self.conn.execute(
            "SELECT id FROM excels WHERE handler=? AND excel_path=?",
            (handler, excel_path),
        ).fetchone()
        if row is None:
            return False
        return Path(excel_path).exists()

    def mark_excel_missing(self, handler: str, excel_path: str) -> None:
        self.conn.execute(
            "UPDATE excels SET status='missing' WHERE handler=? AND excel_path=?",
            (handler, excel_path),
        )
        self.conn.commit()

    # ─── Records (extracted row data) ─────────────────────────────────────────

    def upsert_record(
        self,
        handler: str,
        gna_id: str,
        lta_id: str,
        pdf_filename: str,
        page_number: int,
        data: dict,
    ) -> None:
        """Insert or update a row record with unique key logic."""
        unique_key = self._make_unique_key(gna_id, lta_id, handler, pdf_filename)
        now = datetime.now().isoformat(timespec="seconds")
        data_json = json.dumps(data, ensure_ascii=False, default=str)

        try:
            self.conn.execute(
                """INSERT INTO records (handler, gna_id, lta_id, unique_key, pdf_filename, page_number, data_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(unique_key) DO UPDATE SET
                       data_json=excluded.data_json,
                       page_number=excluded.page_number,
                       created_at=excluded.created_at""",
                (handler, gna_id, lta_id, unique_key, pdf_filename, page_number, data_json, now),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass  # duplicate — ignore

    @staticmethod
    def _make_unique_key(gna_id: str, lta_id: str, handler: str, pdf_filename: str) -> str:
        """
        GNA is unique but repeats with new LTA IDs per PDF.
        Not every GNA has an LTA.
        Uniqueness = gna_id::lta_id_or_NO_LTA::handler::pdf_filename
        """
        safe_gna = (gna_id or "UNKNOWN").strip()
        safe_lta = (lta_id or "NO_LTA").strip()
        if not safe_lta:
            safe_lta = "NO_LTA"
        return f"{safe_gna}::{safe_lta}::{handler}::{pdf_filename}"

    def get_all_records(self, handler: str | None = None) -> list[dict]:
        """Get all records, optionally filtered by handler."""
        if handler:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE handler=? ORDER BY id", (handler,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM records ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    def export_records_as_rows(self, handler: str | None = None) -> list[dict]:
        """Export all records as flat dicts (parsed from data_json)."""
        records = self.get_all_records(handler)
        flat = []
        for r in records:
            try:
                data = json.loads(r["data_json"])
            except (json.JSONDecodeError, TypeError):
                data = {}
            data["_gna_id"] = r["gna_id"]
            data["_lta_id"] = r["lta_id"]
            data["_unique_key"] = r["unique_key"]
            data["_handler"] = r["handler"]
            data["_pdf_filename"] = r["pdf_filename"]
            data["_page_number"] = r["page_number"]
            flat.append(data)
        return flat

    def count_records(self, handler: str | None = None) -> int:
        if handler:
            row = self.conn.execute("SELECT COUNT(*) as cnt FROM records WHERE handler=?", (handler,)).fetchone()
        else:
            row = self.conn.execute("SELECT COUNT(*) as cnt FROM records").fetchone()
        return row["cnt"]

    # ─── Summary / Status ─────────────────────────────────────────────────────

    def summary(self) -> dict:
        """Return a summary of the tracker state."""
        result = {}
        for table in ("downloads", "extractions", "excels", "pipeline_runs", "records"):
            row = self.conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
            result[table] = row["cnt"]
        return result

    def handler_status(self, handler: str) -> dict:
        """Return status for a specific handler."""
        downloads = self.count_downloads(handler)
        extracted = self.count_extractions(handler, "completed")
        pending   = self.count_extractions(handler, "pending")
        failed    = self.count_extractions(handler, "failed")
        records   = self.count_records(handler)
        return {
            "handler": handler,
            "downloads": downloads,
            "extracted": extracted,
            "pending": pending,
            "failed": failed,
            "records": records,
        }

    # ─── Cleanup ──────────────────────────────────────────────────────────────

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
