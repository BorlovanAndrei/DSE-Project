import random
import statistics
import time
from cassandra.cluster import Cluster

N_QUERIES = 500
WARMUP = 50

cluster = Cluster(['192.168.56.101', '192.168.56.102'])
session = cluster.connect('hotel')

# Get real booking_ids for the lookup test.
booking_ids = [r.booking_id for r in
               session.execute("SELECT booking_id FROM bookings_by_id LIMIT 2000")]

queries = [
    ("bookings_by_id (single PK)",
     session.prepare("SELECT * FROM bookings_by_id WHERE booking_id = ?"),
     lambda: (random.choice(booking_ids),)),

    ("bookings_by_status (hot partition)",
     session.prepare("SELECT * FROM bookings_by_status WHERE booking_status = ? LIMIT 100"),
     lambda: (random.choice(['Canceled', 'Not_Canceled']),)),

    ("bookings_by_month (compound PK)",
     session.prepare("SELECT * FROM bookings_by_month WHERE arrival_year = ? AND arrival_month = ? LIMIT 100"),
     lambda: (random.choice([2017, 2018]), random.randint(1, 12))),
]

def pct(values, p):
    s = sorted(values)
    return s[int(round((p / 100.0) * (len(s) - 1)))]

print(f"[*] Running {N_QUERIES} reads per table (first {WARMUP} discarded)...\n")

rows = []
for label, prepared, gen_params in queries:
    latencies = []
    for _ in range(N_QUERIES):
        t0 = time.perf_counter()
        session.execute(prepared, gen_params())
        latencies.append((time.perf_counter() - t0) * 1000.0)
    latencies = latencies[WARMUP:]

    row = {
        'table':   label,
        'mean_ms': round(statistics.mean(latencies), 2),
        'p50_ms':  round(pct(latencies, 50), 2),
        'p95_ms':  round(pct(latencies, 95), 2),
        'p99_ms':  round(pct(latencies, 99), 2),
        'max_ms':  round(max(latencies), 2),
    }
    rows.append(row)
    print(f"{label}")
    print(f"  mean={row['mean_ms']:6.2f}ms  p50={row['p50_ms']:6.2f}ms  "
          f"p95={row['p95_ms']:6.2f}ms  p99={row['p99_ms']:6.2f}ms  max={row['max_ms']:6.2f}ms\n")

with open('b2_results.csv', 'w') as f:
    f.write("table,mean_ms,p50_ms,p95_ms,p99_ms,max_ms\n")
    for r in rows:
        f.write(f"{r['table']},{r['mean_ms']},{r['p50_ms']},{r['p95_ms']},{r['p99_ms']},{r['max_ms']}\n")
print("[ok] Saved b2_results.csv")

cluster.shutdown()