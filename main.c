/*
 * assign_boarding_passes.c
 * Compatible con ISO C90
 * Asigna boarding passes para tickets de un book_ref y muestra resultados
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
    SQLHSTMT stmt_insert, stmt_select;
    SQLRETURN ret;
    char book_ref[64];

    /* Declaraciones al inicio (C90) */
    const char *sql_insert, *sql_select;
    SQLSMALLINT ncols = 0;
    char passenger[256], flight_id[64], sched_dep[64], seat_no[64];

    /* connect */
    ret = odbc_connect(&env, &dbc);
    if (!SQL_SUCCEEDED(ret)) return -1;

    /* disable autocommit */
    ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Failed to disable autocommit\n");
        odbc_disconnect(env, dbc);
        return -1;
    }

    /* Leer book_ref */
    if (fgets(book_ref, sizeof(book_ref), stdin) == NULL) {
        fprintf(stderr, "No book_ref provided\n");
        odbc_disconnect(env, dbc);
        return -1;
    }
    book_ref[strcspn(book_ref, "\r\n")] = '\0';

    /* ------------------------- INSERT ------------------------- */
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_insert);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Failed to allocate statement handle\n");
        odbc_disconnect(env, dbc);
        return -1;
    }

    sql_insert =
        "WITH tickets_to_assign AS ("
        "    SELECT t.ticket_no, tf.flight_id, f.aircraft_code "
        "    FROM tickets t "
        "    JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no "
        "    JOIN flights f ON tf.flight_id = f.flight_id "
        "    LEFT JOIN boarding_passes bp ON tf.ticket_no = bp.ticket_no AND tf.flight_id = bp.flight_id "
        "    WHERE t.book_ref = ? AND bp.boarding_no IS NULL "
        "    ORDER BY t.ticket_no ASC"
        "), available_seats AS ("
        "    SELECT s.seat_no, f.flight_id "
        "    FROM seats s "
        "    JOIN flights f ON f.aircraft_code = s.aircraft_code "
        "    WHERE NOT EXISTS ("
        "        SELECT 1 FROM boarding_passes bp2 "
        "        JOIN ticket_flights tf2 ON bp2.ticket_no = tf2.ticket_no AND bp2.flight_id = tf2.flight_id "
        "        JOIN tickets t2 ON tf2.ticket_no = t2.ticket_no "
        "        WHERE t2.book_ref = ? AND bp2.seat_no = s.seat_no AND tf2.flight_id = f.flight_id"
        "    ) "
        "    ORDER BY s.aircraft_code, s.seat_no "
        "    FOR UPDATE SKIP LOCKED"
        "), assigned_seats AS ("
        "    SELECT t.ticket_no, t.flight_id, a.seat_no, "
        "           ROW_NUMBER() OVER (ORDER BY t.ticket_no, a.seat_no) as boarding_no "
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
        "FROM assigned_seats;";

    ret = SQLPrepare(stmt_insert, (SQLCHAR *)sql_insert, SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error preparing INSERT\n");
        odbc_extract_error("SQLPrepare", stmt_insert, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_insert);
        odbc_disconnect(env, dbc);
        return -1;
    }

    SQLBindParameter(stmt_insert, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(book_ref), 0, book_ref, sizeof(book_ref), NULL);
    SQLBindParameter(stmt_insert, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(book_ref), 0, book_ref, sizeof(book_ref), NULL);

    ret = SQLExecute(stmt_insert);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "INSERT failed\n");
        odbc_extract_error("SQLExecute", stmt_insert, SQL_HANDLE_STMT);
        SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_insert);
        odbc_disconnect(env, dbc);
        return -1;
    }

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Commit failed\n");
        odbc_extract_error("SQLEndTran", dbc, SQL_HANDLE_DBC);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_insert);
        odbc_disconnect(env, dbc);
        return -1;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_insert);

    /* ------------------------- SELECT ------------------------- */
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_select);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Failed to allocate SELECT handle\n");
        odbc_disconnect(env, dbc);
        return -1;
    }

    sql_select =
        "SELECT t.passenger_name, bp.flight_id, f.scheduled_departure, bp.seat_no "
        "FROM boarding_passes bp "
        "JOIN tickets t ON bp.ticket_no = t.ticket_no "
        "JOIN flights f ON bp.flight_id = f.flight_id "
        "WHERE t.book_ref = ? "
        "ORDER BY t.ticket_no ASC, bp.flight_id ASC;";

    ret = SQLPrepare(stmt_select, (SQLCHAR *)sql_select, SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "Error preparing SELECT\n");
        odbc_extract_error("SQLPrepare", stmt_select, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_select);
        odbc_disconnect(env, dbc);
        return -1;
    }

    SQLBindParameter(stmt_select, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(book_ref), 0, book_ref, sizeof(book_ref), NULL);

    ret = SQLExecute(stmt_select);
    if (!SQL_SUCCEEDED(ret)) {
        fprintf(stderr, "SELECT failed\n");
        odbc_extract_error("SQLExecute", stmt_select, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_select);
        odbc_disconnect(env, dbc);
        return -1;
    }

    while (SQL_SUCCEEDED(ret = SQLFetch(stmt_select))) {
        SQLGetData(stmt_select, 1, SQL_C_CHAR, passenger, sizeof(passenger), NULL);
        SQLGetData(stmt_select, 2, SQL_C_CHAR, flight_id, sizeof(flight_id), NULL);
        SQLGetData(stmt_select, 3, SQL_C_CHAR, sched_dep, sizeof(sched_dep), NULL);
        SQLGetData(stmt_select, 4, SQL_C_CHAR, seat_no, sizeof(seat_no), NULL);

        passenger[20] = '\0'; /* truncar */
        printf("%s\t%s\t%s\t%s\n", passenger, flight_id, sched_dep, seat_no);
    }

    /* Cleanup */
    SQLCloseCursor(stmt_select);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt_select);
    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    ret = odbc_disconnect(env, dbc);

    return !SQL_SUCCEEDED(ret);
}
