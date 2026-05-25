import statistics
import time
import uuid
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

N_WRITES = 1000
WARMUP = 50

cluster = Cluster(['192.168.56.101', '192.168.56.102'])
session = cluster.connect('hotel')

LEVELS = [
    ('ONE',    ConsistencyLevel.ONE),
    ('QUORUM', ConsistencyLevel.QUORUM),
    ('ALL',    ConsistencyLevel.ALL),
]

def pct(values, p):
    s = sorted(values)
    return s[int(round((p / 100.0) * (len(s) - 1)))]

rows = []
for cl_name, cl_value in LEVELS:
    stmt = session.prepare("""
        INSERT INTO bookings_by_id
            (booking_id, lead_time, arrival_year, arrival_month,
             avg_price_per_room, booking_status)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    stmt.consistency_level = cl_value

    print(f"[*] Writing {N_WRITES} rows at CL={cl_name} ...")
    latencies = []
    t0 = time.time()
    for i in range(N_WRITES):
        params = (f"BENCH-{uuid.uuid4().hex[:10]}",
                  i % 400, 2018, (i % 12) + 1, 100.0 + (i % 200), 'Not_Canceled')
        s = time.perf_counter()
        session.execute(stmt, params)
        latencies.append((time.perf_counter() - s) * 1000.0)
    elapsed = time.time() - t0

    measured = latencies[WARMUP:]
    row = {
        'consistency':         cl_name,
        'throughput_rows_per_s': round(N_WRITES / elapsed, 1),
        'mean_ms':             round(statistics.mean(measured), 2),
        'p50_ms':              round(pct(measured, 50), 2),
        'p95_ms':              round(pct(measured, 95), 2),
        'p99_ms':              round(pct(measured, 99), 2),
    }
    rows.append(row)
    print(f"  throughput={row['throughput_rows_per_s']:6.0f}/s  "
          f"mean={row['mean_ms']:.2f}ms  p99={row['p99_ms']:.2f}ms\n")

print("=== B3 results ===")
print(f"{'CL':<8} {'throughput/s':>14} {'mean_ms':>10} {'p99_ms':>10}")
for r in rows:
    print(f"{r['consistency']:<8} {r['throughput_rows_per_s']:>14.0f} "
          f"{r['mean_ms']:>10.2f} {r['p99_ms']:>10.2f}")

with open('b3_results.csv', 'w') as f:
    f.write("consistency,throughput_rows_per_s,mean_ms,p50_ms,p95_ms,p99_ms\n")
    for r in rows:
        f.write(f"{r['consistency']},{r['throughput_rows_per_s']},"
                f"{r['mean_ms']},{r['p50_ms']},{r['p95_ms']},{r['p99_ms']}\n")
print("[ok] Saved b3_results.csv")

cluster.shutdown()