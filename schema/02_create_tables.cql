USE hotel;

CREATE TABLE IF NOT EXISTS bookings_by_id (
    booking_id                            text PRIMARY KEY,
    no_of_adults                          int,
    no_of_children                        int,
    no_of_weekend_nights                  int,
    no_of_week_nights                     int,
    type_of_meal_plan                     text,
    required_car_parking_space            int,
    room_type_reserved                    text,
    lead_time                             int,
    arrival_year                          int,
    arrival_month                         int,
    arrival_date                          int,
    market_segment_type                   text,
    repeated_guest                        int,
    no_of_previous_cancellations          int,
    no_of_previous_bookings_not_canceled  int,
    avg_price_per_room                    double,
    no_of_special_requests                int,
    booking_status                        text
);

CREATE TABLE IF NOT EXISTS bookings_by_status (
    booking_status      text,
    booking_id          text,
    lead_time           int,
    avg_price_per_room  double,
    market_segment_type text,
    PRIMARY KEY (booking_status, booking_id)
);

CREATE TABLE IF NOT EXISTS bookings_by_month (
    arrival_year        int,
    arrival_month       int,
    booking_id          text,
    booking_status      text,
    avg_price_per_room  double,
    room_type_reserved  text,
    PRIMARY KEY ((arrival_year, arrival_month), booking_id)
);