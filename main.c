/*
* Created by roberto on 3/5/21.
*/
#include "search.h"
#include "odbc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include </opt/homebrew/include/sql.h>
#include </opt/homebrew/include/sqlext.h>

/**
 * here you need to do your query and fill the choices array of strings
 *
 * @param from form field from
 * @param to form field to
 * @param n_choices fill this with the number of results
 * @param choices fill this with the actual results
 * @param max_length output win maximum width
 * @param max_rows output win maximum number of rows
*/
int main(void)
{
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLRETURN ret;
  char from[512];
  char to[512];
  SQLCHAR buf[512];
  SQLSMALLINT ncols, index = 0;
  

  ret = odbc_connect(&env, &dbc);
  if (!SQL_SUCCEEDED(ret)) {
      return -1;
  }
  
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);


  ret = SQLPrepare(stmt, (SQLCHAR *) "WITH vacancies AS\n\
(\n\
          SELECT    ts.flight_id,\n\
                    ts.n_seats - COALESCE(os.n_seats, 0) AS n_seats\n\
          FROM      (\n\
                             SELECT   flight_id,\n\
                                      Count(*) AS n_seats\n\
                             FROM     flights f \n\
                             NATURAL JOIN     seats s\n\
                             GROUP BY flight_id) ts\n\
          LEFT JOIN\n\
                    (\n\
                             SELECT   flight_id,\n\
                                      Count(*) AS n_seats\n\
                             FROM     boarding_passes \n\
                             NATURAL JOIN     flights\n\
                             GROUP BY flight_id) os\n\
          ON        os.flight_id = ts.flight_id ), direct_flights AS\n\
(\n\
         SELECT   f.flight_id,\n\
                  v.n_seats,\n\
                  0 AS connection_flights,\n\
                  departure_airport,\n\
                  NULL AS connection_airport,\n\
                  arrival_airport,\n\
                  scheduled_departure::                      date,\n\
                  scheduled_arrival::                        date,\n\
                  scheduled_arrival - scheduled_departure AS time_elapsed\n\
         FROM     flights f\n\
         JOIN     vacancies v\n\
         ON       v.flight_id = f.flight_id\n\
         WHERE    departure_airport = ?\n\
         AND      arrival_airport = ?\n\
         AND      scheduled_arrival - scheduled_departure <= interval '1 day'\n\
         ORDER BY time_elapsed ), indirect_flights AS\n\
(\n\
         SELECT   f1.flight_id,\n\
                  Least(v1.n_seats, v2.n_seats) AS n_seats,\n\
                  1                             AS connection_flights,\n\
                  f1.departure_airport,\n\
                  f1.arrival_airport AS connection_airport,\n\
                  f2.arrival_airport,\n\
                  f1.scheduled_departure::                         date,\n\
                  f2.scheduled_arrival::                           date,\n\
                  f2.scheduled_arrival - f1.scheduled_departure AS time_elapsed\n\
         FROM     flights f1\n\
         JOIN     flights f2\n\
         ON       f1.arrival_airport = f2.departure_airport\n\
         JOIN     vacancies v1\n\
         ON       f1.flight_id = v1.flight_id\n\
         JOIN     vacancies v2\n\
         ON       f2.flight_id = v2.flight_id\n\
         WHERE    f1.departure_airport = ?\n\
         AND      f2.arrival_airport = ?\n\
         AND      f2.scheduled_departure >= f1.scheduled_arrival\n\
         AND      f2.scheduled_arrival - f1.scheduled_departure <= interval '1 day'\n\
         ORDER BY time_elapsed ), total_flights AS\n\
(\n\
       SELECT *\n\
       FROM   direct_flights\n\
       UNION\n\
       SELECT *\n\
       FROM   indirect_flights )\n\
SELECT   flight_id,\n\
         n_seats,\n\
         connection_flights,\n\
         scheduled_departure,\n\
         scheduled_arrival,\n\
         time_elapsed\n\
FROM     total_flights\n\
WHERE    n_seats != 0\n\
ORDER BY time_elapsed;", SQL_NTS);

  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "Error preparing statement\n");
    odbc_show_error(SQL_HANDLE_STMT, stmt);
    return -1;
  }

  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(from), 0, from, sizeof(from), NULL);
  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(to), 0, to, sizeof(to), NULL);
  SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(from), 0, from, sizeof(from), NULL);
  SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(to), 0, to, sizeof(to), NULL);

  fgets(from, sizeof(from), stdin);
  from[strcspn(from, "\r\n")] = '\0';

  fgets(to, sizeof(to), stdin);
  to[strcspn(to, "\r\n")] = '\0';

  ret = SQLExecute(stmt);
  if (!SQL_SUCCEEDED(ret)) {
      fprintf(stderr, "Execute failed\n");
      odbc_show_error(SQL_HANDLE_STMT, stmt);
      return -1;
  }
  
  ret = SQLNumResultCols(stmt, &ncols);
  if (!SQL_SUCCEEDED(ret)) {
      fprintf(stderr, "Error getting number of columns\n");
      odbc_show_error(SQL_HANDLE_STMT, stmt);
      return -1;
  }  
  
  /* Loop through the rows in the result-set */
  while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
      for (index = 1 ; index <= ncols ; index++) {
          SQLGetData(stmt, index, SQL_C_CHAR, buf, sizeof(buf), NULL);
          printf("%s", buf);
          if (index < ncols) {
              printf("\t");
          }
      }
      printf("\n");
  }        

    SQLCloseCursor(stmt);

    printf("x = ");
    fflush(stdout);
    printf("\n");

    /* free up statement handle */
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    /* DISCONNECT */
    ret = odbc_disconnect(env, dbc);
    if (!SQL_SUCCEEDED(ret)) {
        return EXIT_FAILURE; 
    }

    return 0;
}

