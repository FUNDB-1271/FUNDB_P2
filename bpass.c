#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include "odbc.h"
#include "lbpass.h"

/*
 * Programa que ejecuta una query compleja con CTEs e INSERT RETURNING
 */

int results_bpass(char * book_ref, int * n_choices, char *** choices, int max_length) {
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN ret; /* Estado de retorno de la API ODBC */
    SQLCHAR passenger_name[32], flight_id[32], seat_no[32], schedule_departure[32];
    int row = 0;
    char buf[512];
    int t = 0;
    SQLSMALLINT num_cols;
    const char *query =
        "WITH max_boarding AS ("
        "    SELECT flight_id, COALESCE(MAX(boarding_no),0) AS max_boarding_no "
        "    FROM boarding_passes "
        "    GROUP BY flight_id"
        "), "
        "tickets_without_bp AS ("
        "    SELECT "
        "        t.ticket_no, "
        "        tf.flight_id, "
        "        f.scheduled_departure, "
        "        f.aircraft_code, "
        "        t.passenger_name "
        "    FROM tickets t "
        "    JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no "
        "    JOIN flights f ON tf.flight_id = f.flight_id "
        "    WHERE t.book_ref = ? "
        "      AND NOT EXISTS ("
        "          SELECT 1 "
        "          FROM boarding_passes bp "
        "          WHERE bp.ticket_no = tf.ticket_no "
        "            AND bp.flight_id = tf.flight_id"
        "      )"
        "), "
        "tickets_to_assign AS ("
        "    SELECT "
        "        t.ticket_no, "
        "        t.flight_id, "
        "        t.scheduled_departure, "
        "        t.aircraft_code, "
        "        t.passenger_name, "
        "        ROW_NUMBER() OVER (PARTITION BY t.flight_id ORDER BY t.ticket_no) AS rn_ticket "
        "    FROM tickets_without_bp t"
        "), "
        "tickets_with_boarding AS ("
        "    SELECT "
        "        t.ticket_no, "
        "        t.flight_id, "
        "        t.scheduled_departure, "
        "        t.aircraft_code, "
        "        t.passenger_name, "
        "        t.rn_ticket + COALESCE(mb.max_boarding_no,0) AS boarding_no, "
        "        t.rn_ticket "
        "    FROM tickets_to_assign t "
        "    LEFT JOIN max_boarding mb ON t.flight_id = mb.flight_id"
        "), "
        "available_seats AS ("
        "    SELECT "
        "        s.seat_no, "
        "        f.flight_id, "
        "        ROW_NUMBER() OVER (PARTITION BY f.flight_id ORDER BY s.seat_no) AS rn_seat "
        "    FROM seats s "
        "    JOIN flights f ON s.aircraft_code = f.aircraft_code "
        "    LEFT JOIN boarding_passes bp ON s.seat_no = bp.seat_no AND f.flight_id = bp.flight_id "
        "    WHERE bp.seat_no IS NULL"
        "), "
        "inserted AS ("
        "    INSERT INTO boarding_passes (ticket_no, flight_id, boarding_no, seat_no) "
        "    SELECT "
        "        t.ticket_no, "
        "        t.flight_id, "
        "        t.boarding_no, "
        "        a.seat_no "
        "    FROM tickets_with_boarding t "
        "    JOIN available_seats a "
        "        ON t.flight_id = a.flight_id AND t.rn_ticket = a.rn_seat "
        "    RETURNING ticket_no, flight_id, boarding_no, seat_no"
        ") "
        "SELECT "
        "    t.passenger_name, "
        "    i.flight_id, "
        "    t.scheduled_departure, "
        "    i.seat_no "
        "FROM inserted i "
        "JOIN tickets_with_boarding t "
        "    ON i.ticket_no = t.ticket_no AND i.flight_id = t.flight_id "
        "ORDER BY t.flight_id, i.boarding_no;";



    
    /* Conectarse */
    ret = odbc_connect(&env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error: no se pudo conectar a la base de datos.\n");
        return EXIT_FAILURE;
    }

    /* Crear un manejador de sentencia */
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    SQLPrepare(stmt, (SQLCHAR*) query, SQL_NTS);


    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(book_ref), 0, book_ref, sizeof(book_ref), NULL);
    SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(book_ref), 0, book_ref, sizeof(book_ref), NULL);

    /* Preparar y ejecutar */
    ret = SQLExecute(stmt);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error al ejecutar la query.\n");
        odbc_extract_error("SQLExecDirect", stmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        odbc_disconnect(env, dbc);
        return EXIT_FAILURE;
    }

    /* Enlazar las columnas retornadas */
    SQLBindCol(stmt, 1, SQL_C_CHAR, passenger_name, sizeof(passenger_name), NULL);
    SQLBindCol(stmt, 2, SQL_C_CHAR, flight_id, sizeof(flight_id), NULL);
    SQLBindCol(stmt, 3, SQL_C_CHAR, schedule_departure, sizeof(schedule_departure), NULL);
    SQLBindCol(stmt, 4, SQL_C_CHAR, seat_no, sizeof(seat_no), NULL);

    SQLNumResultCols(stmt, &num_cols);

    /* Leer y mostrar resultados */
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt)) && row < MAX_RESULTS) { /* importante que el tope del bucle sea MAX_RESULTS para guardar más de una página */
        sprintf(buf, "%-25s %-25s%-25s%-25s",
           passenger_name, flight_id, schedule_departure, seat_no);
        
        t = strlen(buf)+1;
        t = MIN(t, max_length);

        strncpy((*choices)[row], (char*)buf, max_length - 1);
        (*choices)[row][max_length - 1] = '\0';
        row++;
    }

    
    *n_choices = row;
    SQLCloseCursor(stmt);

    fflush(stdout);
    printf("\n"); 

    /* Liberar recursos */
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    ret = odbc_disconnect(env, dbc);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error al desconectar.\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
