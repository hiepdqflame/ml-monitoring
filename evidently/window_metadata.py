"""Feature-only window identity plus server-side ingestion provenance."""
import hashlib
import json
import time


def describe_window(rows, features, epoch, cursor, reference_loaded, now=None):
    now = time.time() if now is None else now
    values = [{key: row.get(key) for key in sorted(features)} for row in rows]
    payload = json.dumps(values, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return {
        'window_id': hashlib.sha256(payload.encode()).hexdigest(),
        'sample_count': len(rows), 'epoch': epoch, 'cursor': cursor,
        'reference_loaded': reference_loaded,
        'latest_age_seconds': max(0, now - rows[-1]['_received_at']) if rows else None,
    }
