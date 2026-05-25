import time
import uuid
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

N_ROWS = 10000
CONCURRENCY = 20

cluster = Cluster(['192.168.56.101', '192.168.56.102'])
session = cluster.connect('hotel')

stmt = session.prepare("""
    INSERT INTO bookings_by_id
        (booking_id, lead_time, arrival_year, arrival_month,
         avg_price_per_room, booking_status)
    VALUES (?, ?, ?, ?, ?, ?)
""")

params = [
    (f"BENCH-{uuid.uuid4().hex[:10]}",
     i % 400, 2018, (i % 12) + 1, 100.0 + (i % 200), 'Not_Canceled')
    for i in range(N_ROWS)
]

print(f"[*] Writing {N_ROWS} rows at concurrency={CONCURRENCY} ...")
t0 = time.time()
results = execute_concurrent_with_args(
    session, stmt, params, concurrency=CONCURRENCY, raise_on_first_error=False,
)
elapsed = time.time() - t0

ok = sum(1 for r in results if r.success)
failed = N_ROWS - ok
throughput = ok / elapsed

print(f"\n=== B1 results ===")
print(f"Rows written:   {ok} / {N_ROWS}  (failed: {failed})")
print(f"Elapsed:        {elapsed:.2f} s")
print(f"Throughput:     {throughput:,.0f} rows/s")

with open('b1_results.csv', 'w') as f:
    f.write("metric,value\n")
    f.write(f"rows_written,{ok}\n")
    f.write(f"rows_failed,{failed}\n")
    f.write(f"elapsed_s,{elapsed:.2f}\n")
    f.write(f"throughput_rows_per_s,{throughput:.0f}\n")
print("[ok] Saved b1_results.csv")

cluster.shutdown()