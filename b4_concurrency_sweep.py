import csv
import statistics
import time
import uuid
import random
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.concurrent import execute_concurrent_with_args

CONTACT_POINTS    = ['192.168.56.101', '192.168.56.102']
KEYSPACE          = 'hotel'
N_ROWS            = 2000       
CONCURRENCY_GRID  = [1, 5, 10, 20, 40, 80, 160]
WARMUP_ROWS       = 100         
RESULTS_CSV       = 'results/b4_concurrency_sweep.csv'


def percentile(values, p):
    s = sorted(values)
    if not s:
        return 0.0
    k = int(round((p / 100.0) * (len(s) - 1)))
    return s[k]


def make_params(n):
    """Build n synthetic INSERT parameter tuples using random booking_ids."""
    params = []
    for _ in range(n):
        bid = f"BMK-{uuid.uuid4().hex[:10]}"
        params.append((
            bid, 2, 0, 1, 2, "Meal Plan 1", 0, "Room_Type 1", 30,
            2018, random.randint(1, 12), random.randint(1, 28),
            "Online", 0, 0, 0, 95.5, 1, "Not_Canceled"
        ))
    return params


def measure_level(session, prepared, concurrency, n_rows):
    """Run n_rows inserts at the given concurrency. Return (throughput, latencies, timeouts)."""
    params = make_params(n_rows)
    latencies = []
    timeouts  = 0

    t0 = time.perf_counter()

    for batch_start in range(0, n_rows, concurrency):
        batch = params[batch_start:batch_start + concurrency]
        b0 = time.perf_counter()
        results = execute_concurrent_with_args(
            session, prepared, batch,
            concurrency=concurrency,
            raise_on_first_error=False,
        )
        b1 = time.perf_counter()
        per_req_ms = ((b1 - b0) * 1000.0) / max(len(batch), 1)
        for r in results:
            latencies.append(per_req_ms)
            if not r.success:
                timeouts += 1

    elapsed = time.perf_counter() - t0
    throughput = n_rows / elapsed if elapsed else 0.0
    lat_after_warmup = latencies[WARMUP_ROWS:] if len(latencies) > WARMUP_ROWS else latencies
    return throughput, lat_after_warmup, timeouts, elapsed


def main():
    cluster = Cluster(CONTACT_POINTS, load_balancing_policy=DCAwareRoundRobinPolicy())
    session = cluster.connect(KEYSPACE)

    insert = session.prepare("""
        INSERT INTO bookings_by_id
            (booking_id, no_of_adults, no_of_children, no_of_weekend_nights,
             no_of_week_nights, type_of_meal_plan, required_car_parking_space,
             room_type_reserved, lead_time, arrival_year, arrival_month, arrival_date,
             market_segment_type, repeated_guest, no_of_previous_cancellations,
             no_of_previous_bookings_not_canceled, avg_price_per_room,
             no_of_special_requests, booking_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    rows_out = []
    for c in CONCURRENCY_GRID:
        print(f"\n[*] concurrency={c}  rows={N_ROWS} ...")
        try:
            tput, lat, to, elapsed = measure_level(session, insert, c, N_ROWS)
        except Exception as exc:
            print(f"    [ERROR] concurrency={c} aborted: {exc}")
            continue

        row = {
            'concurrency':  c,
            'rows':         N_ROWS,
            'duration_s':   round(elapsed, 3),
            'throughput':   round(tput, 1),
            'mean_ms':      round(statistics.mean(lat), 2) if lat else 0,
            'p50_ms':       round(percentile(lat, 50), 2),
            'p95_ms':       round(percentile(lat, 95), 2),
            'p99_ms':       round(percentile(lat, 99), 2),
            'max_ms':       round(max(lat), 2) if lat else 0,
            'writetimeouts': to,
        }
        rows_out.append(row)
        print(f"    throughput={row['throughput']} rows/s  "
              f"p50={row['p50_ms']}ms  p99={row['p99_ms']}ms  timeouts={to}")

        time.sleep(3)

    import os
    os.makedirs('results', exist_ok=True)
    with open(RESULTS_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=rows_out[0].keys())
        w.writeheader()
        w.writerows(rows_out)
    print(f"\n[ok] Wrote {RESULTS_CSV}")

    print("\n=== B4 summary ===")
    print(f"{'concurrency':>11}  {'throughput':>10}  {'p50':>6}  {'p99':>7}  {'timeouts':>9}")
    for r in rows_out:
        print(f"{r['concurrency']:>11}  {r['throughput']:>10}  "
              f"{r['p50_ms']:>6}  {r['p99_ms']:>7}  {r['writetimeouts']:>9}")

    cluster.shutdown()


if __name__ == "__main__":
    main()
