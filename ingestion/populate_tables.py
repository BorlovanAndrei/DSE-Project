from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import time

cluster = Cluster(['192.168.56.101'])
session = cluster.connect('hotel')
session.default_timeout = 30

print("Reading from bookings_by_id...")
t0 = time.time()
rows = list(session.execute("SELECT * FROM bookings_by_id"))
read_time = time.time() - t0
print(f"  Read {len(rows)} rows in {read_time:.2f}s ({len(rows)/read_time:.0f} rows/s)")


def bulk_insert(name, prepared, params, concurrency=20, max_retries=5):
    """Insert with retry on WriteTimeout. Smaller concurrency = less pressure on Node 2."""
    print(f"\nPopulating {name}...")
    t0 = time.time()
    remaining = params
    attempt = 0
    while remaining and attempt < max_retries:
        attempt += 1
        if attempt > 1:
            print(f"  Retry {attempt}/{max_retries} for {len(remaining)} remaining rows...")
            time.sleep(2)
        results = execute_concurrent_with_args(
            session, prepared, remaining, concurrency=concurrency, raise_on_first_error=False
        )
        failed = [remaining[i] for i, r in enumerate(results) if not r.success]
        remaining = failed
    elapsed = time.time() - t0
    total = len(params) - len(remaining)
    print(f"  Inserted {total}/{len(params)} rows in {elapsed:.2f}s "
          f"({total/elapsed:.0f} rows/s); failed permanently: {len(remaining)}")


insert_status = session.prepare("""
    INSERT INTO bookings_by_status
    (booking_status, booking_id, lead_time, avg_price_per_room,
     market_segment_type, room_type_reserved, arrival_year, arrival_month)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")
params_status = [
    (r.booking_status, r.booking_id, r.lead_time, r.avg_price_per_room,
     r.market_segment_type, r.room_type_reserved, r.arrival_year, r.arrival_month)
    for r in rows
]
bulk_insert("bookings_by_status", insert_status, params_status)

insert_month = session.prepare("""
    INSERT INTO bookings_by_month
    (arrival_year, arrival_month, booking_id, booking_status,
     avg_price_per_room, room_type_reserved, no_of_adults, no_of_children)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")
params_month = [
    (r.arrival_year, r.arrival_month, r.booking_id, r.booking_status,
     r.avg_price_per_room, r.room_type_reserved, r.no_of_adults, r.no_of_children)
    for r in rows
]
bulk_insert("bookings_by_month", insert_month, params_month)

cluster.shutdown()
print("\nDone.")
EOF