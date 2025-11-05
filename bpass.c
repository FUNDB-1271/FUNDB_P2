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

int results_bpass(char * book_ref, int * n_choices, char *** choices, int max_length, int max_rows) {
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN ret; /* Estado de retorno de la API ODBC */
    SQLCHAR ticket_no[32], flight_id[32], seat_no[32];
    SQLINTEGER boarding_no;
    int row = 0;
    char buf[512];
    int t = 0;

    /* Conectarse */
    ret = odbc_connect(&env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error: no se pudo conectar a la base de datos.\n");
        return EXIT_FAILURE;
    }

    /* Query con CTEs */
    const char *query =
        "WITH tickets_to_assign AS ("
        "    SELECT t.ticket_no, tf.flight_id, f.scheduled_departure, f.aircraft_code, t.passenger_name "
        "    FROM tickets t "
        "    JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no "
        "    LEFT JOIN boarding_passes bp ON tf.ticket_no = bp.ticket_no AND tf.flight_id = bp.flight_id "
        "    JOIN flights f ON tf.flight_id = f.flight_id "
        "    WHERE t.book_ref = ? AND bp.boarding_no IS NULL "
        "    ORDER BY t.ticket_no ASC"
        "), "
        "available_seats AS ("
        "    SELECT s.seat_no, f.flight_id "
        "    FROM seats s "
        "    JOIN aircrafts_data ad ON s.aircraft_code = ad.aircraft_code "
        "    JOIN flights f ON f.aircraft_code = ad.aircraft_code "
        "    JOIN ticket_flights tf ON tf.flight_id = f.flight_id "
        "    JOIN tickets t ON tf.ticket_no = t.ticket_no "
        "    LEFT JOIN boarding_passes bp ON bp.ticket_no = tf.ticket_no AND bp.flight_id = tf.flight_id "
        "    WHERE t.book_ref = ? AND bp.boarding_no IS NULL "
        "    ORDER BY s.aircraft_code, s.seat_no"
        "), "
        "assigned_seats AS ("
        "    SELECT t.ticket_no, t.flight_id, a.seat_no, "
        "           ROW_NUMBER() OVER (ORDER BY t.ticket_no, a.seat_no) AS boarding_no "
        "    FROM tickets_to_assign t "
        "    JOIN LATERAL ("
        "        SELECT seat_no FROM available_seats a WHERE a.flight_id = t.flight_id LIMIT 1"
        "    ) a ON TRUE"
        ") "
        "INSERT INTO boarding_passes (ticket_no, flight_id, boarding_no, seat_no) "
        "SELECT ticket_no, flight_id, boarding_no, seat_no FROM assigned_seats "
        "RETURNING ticket_no, flight_id, boarding_no, seat_no;";


    
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
    SQLBindCol(stmt, 1, SQL_C_CHAR, ticket_no, sizeof(ticket_no), NULL);
    SQLBindCol(stmt, 2, SQL_C_CHAR, flight_id, sizeof(flight_id), NULL);
    SQLBindCol(stmt, 3, SQL_C_SLONG, &boarding_no, 0, NULL);
    SQLBindCol(stmt, 4, SQL_C_CHAR, seat_no, sizeof(seat_no), NULL);


    /* Leer y mostrar resultados */
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt)) && row < max_rows) {
        sprintf(buf, "%s\t%s\t%d\t%s\n",
               ticket_no, flight_id, boarding_no, seat_no);
        
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
