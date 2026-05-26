import csv
import os
import statistics
import time
import uuid
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.concurrent import execute_concurrent_with_args

CONTACT_POINTS = ['192.168.56.101', '192.168.56.102']
KEYSPACE       = 'hotel'
PARTITION_KEY  = 'bench-partition-A'
N_KEEP         = 200           
N_DELETE       = 2000          
N_READ_SAMPLES = 300           
RESULTS_CSV    = 'results/b5_tombstone_overhead.csv'


def percentile(values, p):
    s = sorted(values)
    if not s:
        return 0.0
    k = int(round((p / 100.0) * (len(s) - 1)))
    return s[k]


def ensure_table(session):
    """Create a dedicated table so the bookings_* tables stay clean.
       gc_grace_seconds=0 makes the experiment self-contained — tombstones
       become eligible for collection sooner, but they still block reads
       until compaction sweeps them. For this measurement that's fine."""
    session.execute("""
        CREATE TABLE IF NOT EXISTS b5_tombstones (
            partition_key text,
            row_id text,
            payload text,
            PRIMARY KEY (partition_key, row_id)
        ) WITH gc_grace_seconds = 0
    """)


def insert_rows(session, prepared, partition, count, prefix):
    params = [(partition, f"{prefix}-{uuid.uuid4().hex[:10]}", "x" * 32)
              for _ in range(count)]
    execute_concurrent_with_args(session, prepared, params,
                                 concurrency=20, raise_on_first_error=False)


def delete_rows(session, prepared, partition, count, prefix):
    """Delete count rows by full PK to produce CELL-level tombstones."""
    rows = list(session.execute(
        f"SELECT row_id FROM b5_tombstones WHERE partition_key = '{partition}' "
        f"AND row_id > '{prefix}' AND row_id < '{prefix}~' LIMIT {count}",
        timeout=60))
    params = [(partition, r.row_id) for r in rows]
    execute_concurrent_with_args(session, prepared, params,
                                 concurrency=20, raise_on_first_error=False)


def measure_reads(session, partition, n_samples):
    """Issue n_samples SELECTs against the partition, return list of ms."""
    prepared = session.prepare(
        "SELECT row_id, payload FROM b5_tombstones WHERE partition_key = ?")
    latencies = []
    for i in range(n_samples):
        t0 = time.perf_counter()
        list(session.execute(prepared, (partition,), timeout=30))
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)
    return latencies[30:] if len(latencies) > 30 else latencies


def summarise(label, latencies, rows_out):
    row = {
        'phase':       label,
        'samples':     len(latencies),
        'mean_ms':     round(statistics.mean(latencies), 3),
        'p50_ms':      round(percentile(latencies, 50), 3),
        'p95_ms':      round(percentile(latencies, 95), 3),
        'p99_ms':      round(percentile(latencies, 99), 3),
        'max_ms':      round(max(latencies), 3),
    }
    rows_out.append(row)
    print(f"  {label:>32}  mean={row['mean_ms']:>7}ms  "
          f"p95={row['p95_ms']:>7}ms  p99={row['p99_ms']:>7}ms")
    return row


def main():
    cluster = Cluster(CONTACT_POINTS, load_balancing_policy=DCAwareRoundRobinPolicy())
    session = cluster.connect(KEYSPACE)
    ensure_table(session)

    insert = session.prepare(
        "INSERT INTO b5_tombstones (partition_key, row_id, payload) VALUES (?, ?, ?)")
    delete = session.prepare(
        "DELETE FROM b5_tombstones WHERE partition_key = ? AND row_id = ?")

    session.execute(f"DELETE FROM b5_tombstones WHERE partition_key = '{PARTITION_KEY}'")
    time.sleep(2)

    rows_out = []

    print(f"\n[Phase 1] Inserting {N_KEEP} live rows ...")
    insert_rows(session, insert, PARTITION_KEY, N_KEEP, "keep")
    time.sleep(3) 

    print(f"[Phase 1] Measuring {N_READ_SAMPLES} reads (no tombstones) ...")
    lat_before = measure_reads(session, PARTITION_KEY, N_READ_SAMPLES)
    summarise("Phase 1 — baseline (no tombstones)", lat_before, rows_out)

    print(f"\n[Phase 2] Inserting {N_DELETE} extra rows then DELETING them ...")
    insert_rows(session, insert, PARTITION_KEY, N_DELETE, "ghost")
    time.sleep(2)
    delete_rows(session, delete, PARTITION_KEY, N_DELETE, "ghost")
    time.sleep(5)   

    print(f"[Phase 2] Measuring {N_READ_SAMPLES} reads (with ~{N_DELETE} tombstones) ...")
    lat_after = measure_reads(session, PARTITION_KEY, N_READ_SAMPLES)
    summarise(f"Phase 2 — after {N_DELETE} deletes", lat_after, rows_out)

    if lat_before and lat_after:
        ratio_mean = statistics.mean(lat_after) / statistics.mean(lat_before)
        ratio_p99  = percentile(lat_after, 99) / max(percentile(lat_before, 99), 0.001)
        rows_out.append({
            'phase':    'ratio_after_over_before',
            'samples':  '-',
            'mean_ms':  round(ratio_mean, 3),
            'p50_ms':   '-',
            'p95_ms':   '-',
            'p99_ms':   round(ratio_p99, 3),
            'max_ms':   '-',
        })
        print(f"\n  Tombstone read penalty:  mean x{ratio_mean:.2f}   p99 x{ratio_p99:.2f}")

    os.makedirs('results', exist_ok=True)
    with open(RESULTS_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=rows_out[0].keys())
        w.writeheader()
        w.writerows(rows_out)
    print(f"\n[ok] Wrote {RESULTS_CSV}")

    cluster.shutdown()


if __name__ == "__main__":
    main()
