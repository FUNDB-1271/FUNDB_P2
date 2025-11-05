/*
 * bpass.c
 * Asigna boarding passes y llena el array de resultados para mostrar en win_out
 */

#include "lbpass.h"
#include "odbc.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

void results_bpass(char *bookID, int *n_choices, char ***choices,
                   int max_length, int max_rows)
{
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt_insert, stmt_select;
    SQLRETURN ret;
    char passenger[256], flight_id[64], sched_dep[64], seat_no[64];
    int i = 0;

    *n_choices = 0;

    /* Connect */
    ret = odbc_connect(&env, &dbc);
    if (!SQL_SUCCEEDED(ret)) return;

    /* Disable autocommit */
    ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    if (!SQL_SUCCEEDED(ret)) {
        odbc_disconnect(env, dbc);
        return;
    }

    /* ------------------------- INSERT ------------------------- */
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_insert);
    if (!SQL_SUCCEEDED(ret)) {
        odbc_disconnect(env, dbc);
        return;
    }

    const char *sql_insert =
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

    SQLPrepare(stmt_insert, (SQLCHAR *)sql_insert, SQL_NTS);
    SQLBindParameter(stmt_insert, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 64, 0, bookID, 0, NULL);
    SQLBindParameter(stmt_insert, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 64, 0, bookID, 0, NULL);

    ret = SQLExecute(stmt_insert);
    if (!SQL_SUCCEEDED(ret)) {
        SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_insert);
        odbc_disconnect(env, dbc);
        return;
    }

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt_insert);

    /* ------------------------- SELECT ------------------------- */
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_select);
    if (!SQL_SUCCEEDED(ret)) {
        odbc_disconnect(env, dbc);
        return;
    }

    const char *sql_select =
        "SELECT t.passenger_name, bp.flight_id, f.scheduled_departure, bp.seat_no "
        "FROM boarding_passes bp "
        "JOIN tickets t ON bp.ticket_no = t.ticket_no "
        "JOIN flights f ON bp.flight_id = f.flight_id "
        "WHERE t.book_ref = ? "
        "ORDER BY t.ticket_no ASC, bp.flight_id ASC;";

    SQLPrepare(stmt_select, (SQLCHAR *)sql_select, SQL_NTS);
    SQLBindParameter(stmt_select, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 64, 0, bookID, 0, NULL);

    ret = SQLExecute(stmt_select);
    if (!SQL_SUCCEEDED(ret)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_select);
        odbc_disconnect(env, dbc);
        return;
    }

    while (SQL_SUCCEEDED(ret = SQLFetch(stmt_select)) && i < max_rows) {
        SQLGetData(stmt_select, 1, SQL_C_CHAR, passenger, sizeof(passenger), NULL);
        SQLGetData(stmt_select, 2, SQL_C_CHAR, flight_id, sizeof(flight_id), NULL);
        SQLGetData(stmt_select, 3, SQL_C_CHAR, sched_dep, sizeof(sched_dep), NULL);
        SQLGetData(stmt_select, 4, SQL_C_CHAR, seat_no, sizeof(seat_no), NULL);

        passenger[20] = '\0'; /* truncar a 20 caracteres */
        snprintf((*choices)[i], max_length, "%s\t%s\t%s\t%s", passenger, flight_id, sched_dep, seat_no);
        i++;
    }

    *n_choices = i;

    /* Cleanup */
    SQLCloseCursor(stmt_select);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt_select);
    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    odbc_disconnect(env, dbc);
}
