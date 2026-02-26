"""SQLite storage layer for scored records."""

import os
import json
import sqlite3
from datetime import datetime, timezone


_db = None

DB_PATH = os.getenv('DATABASE_PATH', os.path.join(os.path.dirname(__file__), '..', 'scores.db'))


def _get_db():
    """Return a shared SQLite connection (created once)."""
    global _db
    if _db is not None:
        return _db

    _db = sqlite3.connect(DB_PATH, check_same_thread=False)
    _db.row_factory = sqlite3.Row
    _db.execute("PRAGMA journal_mode=WAL")
    _init_tables(_db)
    return _db


def _init_tables(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS scored_records (
            hubspot_record_id TEXT PRIMARY KEY,
            lead_type         TEXT NOT NULL,
            score             REAL NOT NULL,
            sub_scores        TEXT NOT NULL,
            modules_run       TEXT NOT NULL,
            weights_used      TEXT NOT NULL,
            raw_inputs        TEXT NOT NULL DEFAULT '{}',
            scored_at         TEXT NOT NULL,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    db.commit()


def upsert_score(record):
    """Insert or replace a scored record. Expects the standard output dict."""
    db = _get_db()
    now = datetime.now(timezone.utc).isoformat()

    db.execute("""
        INSERT INTO scored_records
            (hubspot_record_id, lead_type, score, sub_scores, modules_run, weights_used, raw_inputs, scored_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(hubspot_record_id) DO UPDATE SET
            lead_type     = excluded.lead_type,
            score         = excluded.score,
            sub_scores    = excluded.sub_scores,
            modules_run   = excluded.modules_run,
            weights_used  = excluded.weights_used,
            raw_inputs    = excluded.raw_inputs,
            scored_at     = excluded.scored_at,
            updated_at    = excluded.updated_at
    """, (
        str(record['hubspot_record_id']),
        record['lead_type'],
        record['score'],
        json.dumps(record['sub_scores']),
        json.dumps(record['modules_run']),
        json.dumps(record['weights_used']),
        json.dumps(record.get('raw_inputs', {})),
        record['scored_at'],
        now,
    ))
    db.commit()


def get_score(hubspot_record_id):
    """Fetch a scored record by HubSpot ID. Returns dict or None."""
    db = _get_db()
    row = db.execute(
        "SELECT * FROM scored_records WHERE hubspot_record_id = ?",
        (str(hubspot_record_id),)
    ).fetchone()

    if row is None:
        return None

    return {
        'hubspot_record_id': row['hubspot_record_id'],
        'lead_type': row['lead_type'],
        'score': row['score'],
        'sub_scores': json.loads(row['sub_scores']),
        'modules_run': json.loads(row['modules_run']),
        'weights_used': json.loads(row['weights_used']),
        'raw_inputs': json.loads(row['raw_inputs']),
        'scored_at': row['scored_at'],
    }


def get_all_scores(limit=100):
    """Return the most recently scored records."""
    db = _get_db()
    rows = db.execute(
        "SELECT * FROM scored_records ORDER BY scored_at DESC LIMIT ?",
        (limit,)
    ).fetchall()

    return [
        {
            'hubspot_record_id': row['hubspot_record_id'],
            'lead_type': row['lead_type'],
            'score': row['score'],
            'sub_scores': json.loads(row['sub_scores']),
            'modules_run': json.loads(row['modules_run']),
            'weights_used': json.loads(row['weights_used']),
            'scored_at': row['scored_at'],
        }
        for row in rows
    ]
