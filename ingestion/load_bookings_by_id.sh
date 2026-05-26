#!/usr/bin/env bash

set -e

CSV_PATH="${1:-/home/cass-node-1/hotel_reservations.csv}"

time CQLSH_PYTHON=/usr/bin/python3.11 cqlsh 192.168.56.101 -e "
COPY hotel.bookings_by_id (
    booking_id, no_of_adults, no_of_children, no_of_weekend_nights,
    no_of_week_nights, type_of_meal_plan, required_car_parking_space,
    room_type_reserved, lead_time, arrival_year, arrival_month, arrival_date,
    market_segment_type, repeated_guest, no_of_previous_cancellations,
    no_of_previous_bookings_not_canceled, avg_price_per_room,
    no_of_special_requests, booking_status
) FROM '${CSV_PATH}' WITH HEADER = TRUE;
"