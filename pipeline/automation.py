"""Persistent demo automation policy, shared by sequential Airflow task processes."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import time


class AutomationStore:
    def __init__(self, path=None):
        self.path = Path(path or Path(os.getenv('PIPELINE_DATA_DIR', '/opt/airflow/pipeline_data')) / 'automation.sqlite')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS retrains (window TEXT PRIMARY KEY, parent TEXT UNIQUE, '
                       'run_id TEXT UNIQUE, requested REAL, day TEXT, dispatched INTEGER NOT NULL DEFAULT 0)')
            if 'dispatched' not in {row[1] for row in db.execute('PRAGMA table_info(retrains)')}:
                # Earlier demo reservations predate crash recovery and were already dispatched.
                db.execute('ALTER TABLE retrains ADD COLUMN dispatched INTEGER NOT NULL DEFAULT 1')

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=30)
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _get(db, key):
        row = db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _put(db, key, value):
        db.execute('INSERT OR REPLACE INTO state VALUES (?, ?)', (key, json.dumps(value)))

    def analysis_reason(self, window, min_samples=30, max_age=7200):
        if not window['reference_loaded']:
            return 'no_reference'
        if window['sample_count'] < min_samples:
            return 'insufficient_samples'
        if window['latest_age_seconds'] is None or window['latest_age_seconds'] > max_age:
            return 'stale_data'
        with self.transaction() as db:
            previous = self._get(db, 'analysis')
        if previous:
            if previous['window_id'] == window['window_id']:
                return 'unchanged_window'
            if previous['epoch'] == window['epoch'] and window['cursor'] - previous['cursor'] < min_samples:
                return 'insufficient_new_samples'
        return 'ready'

    def record_analysis(self, window):
        with self.transaction() as db:
            self._put(db, 'analysis', {**window, 'pending_decision': True})

    def recover_analysis(self, window):
        with self.transaction() as db:
            previous = self._get(db, 'analysis')
        if (previous and previous.get('pending_decision') and 'drift_detected' in previous
                and previous['window_id'] == window['window_id']):
            return {key: value for key, value in previous.items() if key != 'pending_decision'}
        return None

    def record_decision(self, window_id, decision):
        if decision['trigger'] or decision['reason'] in ('training_paused_or_missing', 'training_active'):
            return
        with self.transaction() as db:
            previous = self._get(db, 'analysis')
            if previous and previous['window_id'] == window_id:
                self._put(db, 'analysis', {**previous, 'pending_decision': False})

    def complete_dispatch(self, window_id):
        with self.transaction() as db:
            db.execute('UPDATE retrains SET dispatched=1 WHERE window=?', (window_id,))
            previous = self._get(db, 'analysis')
            if previous and previous['window_id'] == window_id:
                self._put(db, 'analysis', {**previous, 'pending_decision': False})

    def reserve_retrain(self, result, parent_run, *, now=None, enabled=True,
                        training_busy=False, training_available=True, cooldown=21600, daily_limit=3):
        def blocked(reason):
            return {'trigger': False, 'reason': reason}
        if not enabled:
            return blocked('automatic_retraining_disabled')
        if not result.get('drift_detected'):
            return blocked(result.get('reason', 'no_drift'))
        if not training_available:
            return blocked('training_paused_or_missing')
        now = time.time() if now is None else now
        day = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
        window = result['window_id']
        with self.transaction() as db:
            row = db.execute('SELECT parent, run_id, dispatched FROM retrains WHERE window=?', (window,)).fetchone()
            if row:
                if row[0] == parent_run or not row[2]:
                    return {'trigger': True, 'reason': 'drift', 'run_id': row[1], 'window_id': window}
                return blocked('window_already_requested')
            if training_busy:
                return blocked('training_active')
            latest = db.execute('SELECT MAX(requested) FROM retrains').fetchone()[0]
            if latest is not None and now - latest < cooldown:
                return blocked('cooldown')
            count = db.execute('SELECT COUNT(*) FROM retrains WHERE day=?', (day,)).fetchone()[0]
            if count >= daily_limit:
                return blocked('daily_limit')
            run_id = 'drift__' + window
            db.execute('INSERT INTO retrains VALUES (?, ?, ?, ?, ?, 0)', (window, parent_run, run_id, now, day))
        return {'trigger': True, 'reason': 'drift', 'run_id': run_id, 'window_id': window}

    def health_notification(self, down, now=None, reminder=3600):
        now = time.time() if now is None else now
        down = sorted(down)
        with self.transaction() as db:
            previous = self._get(db, 'health')
        if down and (not previous or previous['down'] != down or now - previous['notified'] >= reminder):
            return {'status': 'DOWN', 'down': down}
        if not down and previous and previous['down']:
            return {'status': 'RECOVERED', 'down': []}
        return None

    def record_health(self, down, now=None):
        with self.transaction() as db:
            self._put(db, 'health', {'down': sorted(down), 'notified': time.time() if now is None else now})
