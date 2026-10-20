"""CDC consumer: snapshot current tables, then stream slot changes into Parquet."""
import argparse
import json
import time
from datetime import datetime, timezone

import psycopg2
import pyarrow as pa
from psycopg2.extras import RealDictCursor

from common import BRONZE, DSN, write_parquet_atomic

SLOT = "ledgerlake_slot"
TABLES = ["customers", "products", "orders"]
CDC_DIR = BRONZE / "cdc"

SCHEMA = pa.schema([
    ("lsn_int", pa.int64()),
    ("lsn", pa.string()),
    ("xid", pa.int64()),
    ("op", pa.string()),            # I, U, D, or S (snapshot)
    ("source_table", pa.string()),
    ("commit_ts", pa.string()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
    ("before", pa.string()),        # JSON of the old row (U, D)
    ("after", pa.string()),         # JSON of the new row (I, U, S)
])

OPTS = "'format-version', '2', 'include-timestamp', '1'"
PEEK_SQL = ("SELECT lsn::text, xid::text, data "
            f"FROM pg_logical_slot_peek_changes(%s, NULL, %s, {OPTS})")
CONFIRM_SQL = ("SELECT count(*) "
               f"FROM pg_logical_slot_get_changes(%s, %s::pg_lsn, NULL, {OPTS})")


def lsn_to_int(lsn):
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def to_dict(columns):
    return {c["name"]: c["value"] for c in columns} if columns else None


def dumps(d):
    return json.dumps(d, default=str) if d is not None else None


def parse_events(rows):
    """Turn raw wal2json rows into flat event dicts (skipping begin/commit markers)."""
    events, commit_ts = [], None
    for lsn, xid, data in rows:
        ev = json.loads(data)
        action = ev["action"]
        if action == "B":
            commit_ts = ev.get("timestamp")
        elif action in ("I", "U", "D") and ev.get("table") in TABLES:
            events.append({
                "lsn_int": lsn_to_int(lsn),
                "lsn": lsn,
                "xid": int(xid) if xid else None,
                "op": action,
                "source_table": ev["table"],
                "commit_ts": commit_ts,
                "before": dumps(to_dict(ev.get("identity"))),
                "after": dumps(to_dict(ev.get("columns"))),
            })
    return events


def write_events(events, prefix=""):
    """One Parquet file per table, in tbl=/dt= folders. File name = LSN range."""
    now = datetime.now(timezone.utc)
    by_table = {}
    for e in events:
        by_table.setdefault(e["source_table"], []).append({**e, "ingested_at": now})
    for tbl, rows in by_table.items():
        table = pa.Table.from_pylist(rows, schema=SCHEMA)
        name = f"{prefix}{rows[0]['lsn_int']:020d}_{rows[-1]['lsn_int']:020d}.parquet"
        write_parquet_atomic(table, CDC_DIR / f"tbl={tbl}" / f"dt={now:%Y-%m-%d}" / name)


def snapshot():
    conn = psycopg2.connect(DSN)
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    events = []
    with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT pg_current_wal_lsn()::text AS lsn")
        lsn = cur.fetchone()["lsn"]
        for tbl in TABLES:
            cur.execute(f"SELECT * FROM {tbl}")   # names come from our own constant list
            for row in cur.fetchall():
                events.append({
                    "lsn_int": lsn_to_int(lsn), "lsn": lsn, "xid": None,
                    "op": "S", "source_table": tbl, "commit_ts": None,
                    "before": None, "after": dumps(row),
                })
    conn.close()
    write_events(events, prefix="snapshot_")
    print(f"snapshot at LSN {lsn}: {len(events)} rows")


def stream(once, batch_size, poll_seconds):
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    total = 0
    with conn.cursor() as cur:
        while True:
            cur.execute(PEEK_SQL, (SLOT, batch_size))       # 1. read, don't consume
            rows = cur.fetchall()
            if not rows:
                if once:
                    break
                time.sleep(poll_seconds)
                continue
            events = parse_events(rows)
            write_events(events)                            # 2. save to bronze
            last_lsn = rows[-1][0]
            cur.execute(CONFIRM_SQL, (SLOT, last_lsn))      # 3. only now move the bookmark
            cur.fetchall()
            total += len(events)
            print(f"wrote {len(events)} events, confirmed up to LSN {last_lsn}")
    print(f"done: {total} events")


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshot")
    s = sub.add_parser("stream")
    s.add_argument("--once", action="store_true", help="drain the slot and exit")
    s.add_argument("--batch-size", type=int, default=500)
    s.add_argument("--poll", type=float, default=2.0)
    args = parser.parse_args()
    try:
        if args.cmd == "snapshot":
            snapshot()
        else:
            stream(args.once, args.batch_size, args.poll)
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()