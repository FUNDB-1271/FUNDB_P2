WITH max_boarding AS (
    SELECT flight_id, COALESCE(MAX(boarding_no),0) AS max_boarding_no
    FROM boarding_passes
    GROUP BY flight_id
),
tickets_without_bp AS (
    SELECT 
        t.ticket_no,
        tf.flight_id,
        f.scheduled_departure,
        f.aircraft_code,
        t.passenger_name
    FROM tickets t
    JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
    JOIN flights f ON tf.flight_id = f.flight_id
    WHERE t.book_ref = '34265B'
      AND NOT EXISTS (
          SELECT 1
          FROM boarding_passes bp
          WHERE bp.ticket_no = tf.ticket_no
            AND bp.flight_id = tf.flight_id
      )
),
tickets_to_assign AS (
    SELECT
        t.ticket_no,
        t.flight_id,
        t.scheduled_departure,
        t.aircraft_code,
        t.passenger_name,
        ROW_NUMBER() OVER (PARTITION BY t.flight_id ORDER BY t.ticket_no) AS rn_ticket
    FROM tickets_without_bp t
),
tickets_with_boarding AS (
    SELECT
        t.ticket_no,
        t.flight_id,
        t.scheduled_departure,
        t.aircraft_code,
        t.passenger_name,
        t.rn_ticket + COALESCE(mb.max_boarding_no,0) AS boarding_no,
        t.rn_ticket 
    FROM tickets_to_assign t
    LEFT JOIN max_boarding mb ON t.flight_id = mb.flight_id
),
available_seats AS (
    SELECT 
        s.seat_no,
        f.flight_id,
        ROW_NUMBER() OVER (PARTITION BY f.flight_id ORDER BY s.seat_no) AS rn_seat
    FROM seats s
    JOIN flights f ON s.aircraft_code = f.aircraft_code
    LEFT JOIN boarding_passes bp ON s.seat_no = bp.seat_no AND f.flight_id = bp.flight_id
    WHERE bp.seat_no IS NULL
),
inserted AS (
    INSERT INTO boarding_passes (ticket_no, flight_id, boarding_no, seat_no)
    SELECT 
        t.ticket_no,
        t.flight_id,
        t.boarding_no,
        a.seat_no
    FROM tickets_with_boarding t
    JOIN available_seats a
        ON t.flight_id = a.flight_id AND t.rn_ticket = a.rn_seat
    RETURNING ticket_no, flight_id, boarding_no, seat_no
)
SELECT 
    t.passenger_name,
    i.flight_id,
    t.scheduled_departure,
    i.seat_no
FROM inserted i
JOIN tickets_with_boarding t
    ON i.ticket_no = t.ticket_no AND i.flight_id = t.flight_id
ORDER BY t.flight_id, i.boarding_no;