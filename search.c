/*
* Created by roberto on 3/5/21.
*/
#include "search.h"

void exit_safely(SQLHSTMT *stmt);

void    results_search(char * from, char *to, char *date,
                       int * n_choices, char *** choices,
                       int max_length,
                       int max_rows)
   /**here you need to do your query and fill the choices array of strings
 *
 * @param from form field from
 * @param to form field to
 * @param date form field date
 * @param n_choices fill this with the number of results
 * @param choices fill this with the actual results
 * @param max_length output win maximum width
 * @param max_rows output win maximum number of rows
  */
{
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret;
  SQLLEN row_ind;

  char buf[512];
  FILE *f = NULL;
  int row = 0;
  int t = 0;
  const char *query = "WITH vacancies AS\n"
"(\n"
"          SELECT    ts.flight_id,\n"
"                    ts.n_seats - COALESCE(os.n_seats, 0) AS n_seats\n"
"          FROM      (\n"
"                             SELECT   flight_id,\n"
"                                      Count(*) AS n_seats\n"
"                             FROM     flights f \n"
"                             NATURAL JOIN     seats s\n"
"                             GROUP BY flight_id) ts\n"
"          LEFT JOIN\n"
"                    (\n"
"                             SELECT   flight_id,\n"
"                                      Count(*) AS n_seats\n"
"                             FROM     boarding_passes \n"
"                             NATURAL JOIN     flights\n"
"                             GROUP BY flight_id) os\n"
"          ON        os.flight_id = ts.flight_id ), direct_flights AS\n"
"(\n"
"         SELECT   f.flight_id,\n"
"                  v.n_seats,\n"
"                  0 AS connection_flights,\n"
"                  departure_airport,\n"
"                  NULL AS connection_airport,\n"
"                  arrival_airport,\n"
"                  scheduled_departure::                      date,\n"
"                  scheduled_arrival::                        date,\n"
"                  scheduled_arrival - scheduled_departure AS time_elapsed\n"
"         FROM     flights f\n"
"         JOIN     vacancies v\n"
"         ON       v.flight_id = f.flight_id\n"
"         WHERE    departure_airport = ?\n"
"         AND      arrival_airport = ?\n"
"         AND      scheduled_arrival - scheduled_departure <= interval '1 day'\n"
"         AND      scheduled_departure::date = ?\n"
"         ORDER BY time_elapsed ), indirect_flights AS\n"
"(\n"
"         SELECT   f1.flight_id,\n"
"                  Least(v1.n_seats, v2.n_seats) AS n_seats,\n"
"                  1                             AS connection_flights,\n"
"                  f1.departure_airport,\n"
"                  f1.arrival_airport AS connection_airport,\n"
"                  f2.arrival_airport,\n"
"                  f1.scheduled_departure::                         date,\n"
"                  f2.scheduled_arrival::                           date,\n"
"                  f2.scheduled_arrival - f1.scheduled_departure AS time_elapsed\n"
"         FROM     flights f1\n"
"         JOIN     flights f2\n"
"         ON       f1.arrival_airport = f2.departure_airport\n"
"         JOIN     vacancies v1\n"
"         ON       f1.flight_id = v1.flight_id\n"
"         JOIN     vacancies v2\n"
"         ON       f2.flight_id = v2.flight_id\n"
"         WHERE    f1.departure_airport = ?\n"
"         AND      f2.arrival_airport = ?\n"
"         AND      f2.scheduled_departure >= f1.scheduled_arrival\n"
"         AND      f2.scheduled_arrival - f1.scheduled_departure <= interval '1 day'\n"
"         AND      f1.scheduled_departure::date = ?\n"
"         ORDER BY time_elapsed ), total_flights AS\n"
"(\n"
"       SELECT *\n"
"       FROM   direct_flights\n"
"       UNION\n"
"       SELECT *\n"
"       FROM   indirect_flights )\n"
"SELECT concat_ws(E'\t',\n"
"                 flight_id::text,\n"
"                 n_seats::text,\n"
"                 connection_flights::text,\n"
"                 scheduled_departure::text,\n"
"                 scheduled_arrival::text,\n"
"                 time_elapsed::text) AS row_text\n"
"FROM     total_flights\n"
"WHERE    n_seats != 0\n"
"ORDER BY time_elapsed;";

  if (!(f = fopen("salida.txt", "w"))) return;

  ret = odbc_connect(&env, &dbc);
  if(!SQL_SUCCEEDED(ret)) {
    return;
  }

  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  ret = SQLPrepare(stmt, (SQLCHAR *) query, SQL_NTS);


  if (!(SQL_SUCCEEDED(ret))) {
    fprintf(stderr, "Error preparando statement\n"); 
    exit_safely(stmt);
    return;
  }

  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(from), 0, from, strlen(from)+1, NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(to), 0, to, strlen(to)+1, NULL);
  SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(date), 0, date, strlen(date)+1, NULL);
  SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(from), 0, from, strlen(from)+1, NULL);
  SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(to), 0, to, strlen(to)+1, NULL);
  SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(date), 0, date, strlen(date)+1, NULL);


  ret = SQLExecute(stmt);
  if (!SQL_SUCCEEDED(ret)) {
      fprintf(f, "Execute failed\n");
      odbc_show_error(f, SQL_HANDLE_STMT, stmt);
      fclose(f);
      return;
  }

  /*
  ret = SQLBindCol(stmt, 1, SQL_C_LONG, flight_id, sizeof(flight_id), NULL);
  ret = SQLBindCol(stmt, 2, SQL_C_LONG, number_of_seats, sizeof(number_of_seats), NULL);
  ret = SQLBindCol(stmt, 3, SQL_C_LONG, connection_flights, sizeof(connection_flights), NULL);
  ret = SQLBindCol(stmt, 4, SQL_C_CHAR, departure_date, sizeof(departure_date), NULL);
  ret = SQLBindCol(stmt, 5, SQL_C_CHAR, arrival_date, sizeof(arrival_date), NULL);
  ret = SQLBindCol(stmt, 6, SQL_C_CHAR, time_elapsed, sizeof(time_elapsed), NULL);      

  while (SQL_SUCCEEDED(SQLFetch(stmt)) && row < max_rows) {
    sprintf(buf, "%s\t%s\t%s\t%s\t%s\t%s\t\n", flight_id, number_of_seats, connection_flights, departure_date, arrival_date, time_elapsed);
    fprintf(f, "%d, %s", row, buf);

    strncpy((*choices)[row], buf, max_length - 1);
    (*choices)[row][max_length - 1] = '\0';
    row++;
  }
*/


  /* after SQLExecute(stmt) */
  ret = SQLBindCol(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &row_ind);
  if (!SQL_SUCCEEDED(ret)) { 
    odbc_show_error(f, SQL_HANDLE_STMT, stmt); 
    /*cleanup*/ 
  }

  while (row < max_rows && SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
      if (row_ind == SQL_NULL_DATA) {
          buf[0] = '\0';
      }
      t = strlen(buf)+1;
      t = MIN(t, max_length);

      /* copy up to max_length-1 characters, ensure NUL termination */
      strncpy((*choices)[row], (char*)buf, max_length - 1);
      (*choices)[row][max_length - 1] = '\0';
      row++;
  }

  *n_choices = row;
  SQLCloseCursor(stmt);
  fclose(f);

  fflush(stdout);
  printf("\n"); 

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);


  ret = odbc_disconnect(env, dbc);
  if (!SQL_SUCCEEDED(ret)) {
      return; 
  }
}

void exit_safely(SQLHSTMT *stmt) {
  if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}
