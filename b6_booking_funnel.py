import csv
import os
import random
import statistics
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

CONTACT_POINTS = ['192.168.56.101', '192.168.56.102']
KEYSPACE       = 'hotel'
N_USERS        = 500       
CONCURRENCY    = 16        
CANCEL_PROB    = 0.30   
RESULTS_CSV    = 'results/b6_booking_funnel.csv'

YEARS  = [2017, 2018]
MONTHS = list(range(1, 13))


def percentile(values, p):
    s = sorted(values)
    if not s:
        return 0.0
    k = int(round((p / 100.0) * (len(s) - 1)))
    return s[k]


def make_session():
    cluster = Cluster(CONTACT_POINTS, load_balancing_policy=DCAwareRoundRobinPolicy())
    session = cluster.connect(KEYSPACE)
    return cluster, session


def prepare_statements(session):
    return {
        'search': session.prepare("""
            SELECT booking_id, booking_status, avg_price_per_room, room_type_reserved
            FROM bookings_by_month
            WHERE arrival_year = ? AND arrival_month = ? LIMIT 20
        """),
        'reserve': session.prepare("""
            INSERT INTO bookings_by_id
                (booking_id, no_of_adults, no_of_children, no_of_weekend_nights,
                 no_of_week_nights, type_of_meal_plan, required_car_parking_space,
                 room_type_reserved, lead_time, arrival_year, arrival_month, arrival_date,
                 market_segment_type, repeated_guest, no_of_previous_cancellations,
                 no_of_previous_bookings_not_canceled, avg_price_per_room,
                 no_of_special_requests, booking_status)
            VALUES (?, 2, 0, 1, 2, 'Meal Plan 1', 0, 'Room_Type 1', 30,
                    ?, ?, ?, 'Online', 0, 0, 0, ?, 1, ?)
        """),
        'lookup': session.prepare("""
            SELECT * FROM bookings_by_id WHERE booking_id = ?
        """),
        'cancel': session.prepare("""
            UPDATE bookings_by_id SET booking_status = 'Canceled'
            WHERE booking_id = ?
        """),
    }


def time_call(session, stmt, params):
    """Execute one statement, return (success, elapsed_ms)."""
    t0 = time.perf_counter()
    try:
        list(session.execute(stmt, params, timeout=30))
        ok = True
    except Exception:
        ok = False
    t1 = time.perf_counter()
    return ok, (t1 - t0) * 1000.0


def run_one_funnel(session, stmts):
    """Simulate one user funnel. Return list of (op_name, ok, elapsed_ms)."""
    events = []

    for _ in range(5):
        y = random.choice(YEARS)
        m = random.choice(MONTHS)
        ok, ms = time_call(session, stmts['search'], (y, m))
        events.append(('search', ok, ms))

    bid = f"BMK6-{uuid.uuid4().hex[:10]}"
    y = random.choice(YEARS)
    m = random.choice(MONTHS)
    d = random.randint(1, 28)
    price = round(random.uniform(60.0, 240.0), 2)
    ok, ms = time_call(session, stmts['reserve'], (bid, y, m, d, price, 'Not_Canceled'))
    events.append(('reserve', ok, ms))

    ok, ms = time_call(session, stmts['lookup'], (bid,))
    events.append(('lookup', ok, ms))

    if random.random() < CANCEL_PROB:
        ok, ms = time_call(session, stmts['cancel'], (bid,))
        events.append(('cancel', ok, ms))

    return events


def main():
    cluster, session = make_session()
    stmts = prepare_statements(session)

    all_events = defaultdict(list)  
    failures   = defaultdict(int)

    print(f"[*] Running {N_USERS} simulated funnels at concurrency = {CONCURRENCY} ...")
    t_run0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(run_one_funnel, session, stmts)
                   for _ in range(N_USERS)]
        for fut in as_completed(futures):
            for op, ok, ms in fut.result():
                if ok:
                    all_events[op].append(ms)
                else:
                    failures[op] += 1

    elapsed = time.perf_counter() - t_run0
    print(f"[ok] Funnels finished in {elapsed:.1f} s.")

    rows_out = []
    total_ops = 0
    for op in ('search', 'reserve', 'lookup', 'cancel'):
        lats = all_events[op]
        total_ops += len(lats)
        if not lats:
            continue
        row = {
            'operation':    op,
            'count':        len(lats),
            'failures':     failures[op],
            'mean_ms':      round(statistics.mean(lats), 2),
            'p50_ms':       round(percentile(lats, 50), 2),
            'p95_ms':       round(percentile(lats, 95), 2),
            'p99_ms':       round(percentile(lats, 99), 2),
            'max_ms':       round(max(lats), 2),
            'throughput_per_s': round(len(lats) / elapsed, 1),
        }
        rows_out.append(row)
        print(f"  {op:>8}: n={row['count']:>5}  "
              f"mean={row['mean_ms']:>6}ms  p95={row['p95_ms']:>6}ms  "
              f"p99={row['p99_ms']:>6}ms  failures={failures[op]}")

    rows_out.append({
        'operation':         'ALL',
        'count':             total_ops,
        'failures':          sum(failures.values()),
        'mean_ms':           '-',
        'p50_ms':            '-',
        'p95_ms':            '-',
        'p99_ms':            '-',
        'max_ms':            '-',
        'throughput_per_s':  round(total_ops / elapsed, 1),
    })
    print(f"\n  TOTAL ops: {total_ops}  over {elapsed:.1f} s  "
          f"= {total_ops / elapsed:.1f} ops/s")

    os.makedirs('results', exist_ok=True)
    with open(RESULTS_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=rows_out[0].keys())
        w.writeheader()
        w.writerows(rows_out)
    print(f"\n[ok] Wrote {RESULTS_CSV}")

    cluster.shutdown()


if __name__ == "__main__":
    main()
