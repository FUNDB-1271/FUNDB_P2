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
)
SELECT 
    t.ticket_no,
    LEFT(t.passenger_name, 20) AS passenger_name,
    t.flight_id,
    t.scheduled_departure,
    a.seat_no
FROM tickets_to_assign t
JOIN LATERAL (
    SELECT seat_no
    FROM available_seats a
    WHERE a.flight_id = t.flight_id
    LIMIT 1
) a ON TRUE
ORDER BY t.ticket_no, a.seat_no asc;
