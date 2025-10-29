WITH tickets_to_assign AS (
    SELECT 
        t.ticket_no,
        tf.flight_id,
        f.scheduled_departure,
        f.aircraft_code,
        t.passenger_name
    FROM tickets t
    JOIN ticket_flights tf 
        ON t.ticket_no = tf.ticket_no
    LEFT JOIN boarding_passes bp
        ON tf.ticket_no = bp.ticket_no
       AND tf.flight_id = bp.flight_id
    JOIN flights f 
        ON tf.flight_id = f.flight_id
    WHERE t.book_ref = '000068' 
      AND bp.boarding_no IS NULL
    ORDER BY t.ticket_no ASC
), 
available_seats AS (
    SELECT s.seat_no, f.flight_id
    FROM seats s
    JOIN aircrafts_data ad 
        ON s.aircraft_code = ad.aircraft_code 
    JOIN flights f
        ON f.aircraft_code = ad.aircraft_code 
    JOIN ticket_flights tf 
        ON tf.flight_id = f.flight_id 
    JOIN tickets t
        ON tf.ticket_no = t.ticket_no
    LEFT JOIN boarding_passes bp 
        ON bp.ticket_no = tf.ticket_no
       AND bp.flight_id = tf.flight_id
    WHERE t.book_ref = '000068'
      AND bp.boarding_no IS NULL
    ORDER BY s.aircraft_code, s.seat_no
),
assigned_seats AS (
    SELECT 
        t.ticket_no,
        t.flight_id,
        a.seat_no,
        ROW_NUMBER() OVER (ORDER BY t.ticket_no, a.seat_no) as boarding_no
    FROM tickets_to_assign t
    JOIN LATERAL (
        SELECT seat_no
        FROM available_seats a
        WHERE a.flight_id = t.flight_id
        LIMIT 1
    ) a ON TRUE
)
INSERT INTO boarding_passes (ticket_no, flight_id, boarding_no, seat_no)
SELECT ticket_no, flight_id, boarding_no, seat_no
FROM assigned_seats
RETURNING ticket_no, flight_id, boarding_no, seat_no;

