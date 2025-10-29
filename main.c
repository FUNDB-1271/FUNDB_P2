/*
 * assign_boarding_passes.c
 * Compatible con ISO C90
 * Asigna boarding passes para tickets de un book_ref
 */

#include "search.h"
#include "odbc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

int main(void)
{
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN ret;
    char book_ref[64];

    /* Declaraciones al inicio (C90) */
    const char *sql;
    SQLSMALLINT ncols = 0;
    char passenger[256], flight_id[64], sched_dep[64], seat_no[64];

    /* connect */
    ret = odbc_connect(&env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
        return -1;
    }

    /* disable autocommit */
    ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Failed to disable autocommit\n");
        odbc_disconnect(env, dbc);
        return -1;
    }

    /* Allocate statement handle */
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Failed to allocate statement handle\n");
        odbc_disconnect(env, dbc);
        return -1;
    }

    /* Query dividida en varias partes */
    sql =
        "WITH tickets_to_assign AS ("
        "    SELECT t.ticket_no, tf.flight_id, f.scheduled_departure, f.aircraft_code, t.passenger_name "
        "    FROM tickets t "
        "    JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no "
        "    LEFT JOIN boarding_passes bp ON tf.ticket_no = bp.ticket_no AND tf.flight_id = bp.flight_id "
        "    JOIN flights f ON tf.flight_id = f.flight_id "
        "    WHERE t.book_ref = ? AND bp.boarding_no IS NULL "
        "    ORDER BY t.ticket_no ASC"
        "), available_seats AS ("
        "    SELECT s.seat_no, f.flight_id, s.aircraft_code /* usar aircraft_code como fila unica */ "
        "    FROM seats s "
        "    JOIN aircrafts_data ad ON s.aircraft_code = ad.aircraft_code "
        "    JOIN flights f ON f.aircraft_code = ad.aircraft_code "
        "    JOIN ticket_flights tf ON tf.flight_id = f.flight_id "
        "    JOIN tickets t ON tf.ticket_no = t.ticket_no "
        "    LEFT JOIN boarding_passes bp ON bp.ticket_no = tf.ticket_no AND bp.flight_id = tf.flight_id "
        "    WHERE t.book_ref = ? AND bp.boarding_no IS NULL "
        "    ORDER BY s.aircraft_code, s.seat_no "
        "    FOR UPDATE SKIP LOCKED"
        "), assigned_seats AS ("
        "    SELECT t.ticket_no, t.flight_id, a.seat_no, "
        "           ROW_NUMBER() OVER (ORDER BY t.ticket_no, a.seat_no) as boarding_no, "
        "           t.passenger_name, t.scheduled_departure "
        "    FROM tickets_to_assign t "
        "    JOIN LATERAL ("
        "        SELECT seat_no "
        "        FROM available_seats a "
        "        WHERE a.flight_id = t.flight_id "
        "        LIMIT 1"
        "    ) a ON TRUE"
        ") "
        "INSERT INTO boarding_passes (ticket_no, flight_id, boarding_no, seat_no) "
        "SELECT ticket_no, flight_id, boarding_no, seat_no "
        "FROM assigned_seats "
        "RETURNING passenger_name, flight_id, scheduled_departure, seat_no;";

    /* Preparar statement */
    ret = SQLPrepare(stmt, (SQLCHAR *)sql, SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error preparing statement\n");
        odbc_extract_error("SQLPrepare", stmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        odbc_disconnect(env, dbc);
        return -1;
    }

    /* Bind parameters */
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(book_ref), 0, book_ref, sizeof(book_ref), NULL);
    SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(book_ref), 0, book_ref, sizeof(book_ref), NULL);

    /* Leer book_ref */
    if (fgets(book_ref, sizeof(book_ref), stdin) == NULL) {
        fprintf(stderr, "No book_ref provided\n");
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        odbc_disconnect(env, dbc);
        return -1;
    }
    book_ref[strcspn(book_ref, "\r\n")] = '\0';

    /* Ejecutar */
    ret = SQLExecute(stmt);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Execute failed\n");
        odbc_extract_error("SQLExecute", stmt, SQL_HANDLE_STMT);
        SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        odbc_disconnect(env, dbc);
        return -1;
    }

    /* Número de columnas */
    ret = SQLNumResultCols(stmt, &ncols);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error getting number of columns\n");
        odbc_extract_error("SQLNumResultCols", stmt, SQL_HANDLE_STMT);
        SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        odbc_disconnect(env, dbc);
        return -1;
    }

    /* Fetch y mostrar resultados */
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
        SQLGetData(stmt, 1, SQL_C_CHAR, passenger, sizeof(passenger), NULL);
        SQLGetData(stmt, 2, SQL_C_CHAR, flight_id, sizeof(flight_id), NULL);
        SQLGetData(stmt, 3, SQL_C_CHAR, sched_dep, sizeof(sched_dep), NULL);
        SQLGetData(stmt, 4, SQL_C_CHAR, seat_no, sizeof(seat_no), NULL);

        passenger[20] = '\0'; /* truncar */
        printf("%s\t%s\t%s\t%s\n", passenger, flight_id, sched_dep, seat_no);
    }

    /* Commit */
    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Commit failed\n");
        odbc_extract_error("SQLEndTran", dbc, SQL_HANDLE_DBC);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        odbc_disconnect(env, dbc);
        return -1;
    }

    /* Cleanup */
    SQLCloseCursor(stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    ret = odbc_disconnect(env, dbc);

    return !SQL_SUCCEEDED(ret);
}
