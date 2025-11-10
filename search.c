/*
* Created by roberto on 3/5/21.
*/
#include "search.h"

void exit_safely(SQLHSTMT *stmt);

void    results_search(char * from, char *to, char *date,
                       int * n_choices, char *** choices,
                       char *** choices_extra, int max_length,
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
  SQLINTEGER flight_id, flight_id2;
  SQLINTEGER number_of_seats;
  SQLINTEGER connection_flights;
  SQLCHAR f1_departure_date[256], f2_departure_date[256];
  SQLCHAR f1_arrival_date[256], f2_arrival_date[256];
  SQLCHAR f1_aircraft_code[256], f2_aircraft_code[256];
  SQLCHAR time_elapsed[256];
  SQLLEN len1, len2, len3, len4, len5, len6, len7, len8, len9, len10, len11;
  FILE *f = NULL;
  char buf1[512], buf2[512];
  int row = 0;
  int t2 = 0, t1 = 0;
  SQLSMALLINT num_cols;
  char header[256] = "";
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
"                  NULL::int AS flight_id2,\n"  
"                  v.n_seats,\n"
"                  0 AS connection_flights,\n"
"                  departure_airport,\n"
"                  NULL AS connection_airport,\n"
"                  arrival_airport,\n"
"                  scheduled_departure AS flight_1_departure,\n"
"                  scheduled_arrival AS flight_1_arrival,\n"
"                  NULL::timestamp AS flight_2_departure,\n"
"                  NULL::timestamp AS flight_2_arrival,\n"
"                  scheduled_arrival - scheduled_departure AS time_elapsed,\n"
"                  f.aircraft_code AS f1_aircraft,\n"
"                  NULL AS f2_aircraft\n"
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
"                  f2.flight_id AS flight_id2,\n"  
"                  Least(v1.n_seats, v2.n_seats) AS n_seats,\n"
"                  1                             AS connection_flights,\n"
"                  f1.departure_airport,\n"
"                  f1.arrival_airport AS connection_airport,\n"
"                  f2.arrival_airport,\n"
"                  f1.scheduled_departure AS flight_1_departure,\n"
"                  f1.scheduled_arrival AS flight_1_arrival,\n"
"                  f2.scheduled_departure AS flight_2_departure,\n"
"                  f2.scheduled_arrival AS flight_2_arrival,\n"
"                  f2.scheduled_arrival - f1.scheduled_departure AS time_elapsed,\n"
"                  f1.aircraft_code AS f1_aircraft,\n"
"                  f2.aircraft_code AS f2_aircraft\n"
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
"SELECT   flight_id AS \"Flight\",\n"
"         n_seats AS \"Seat\",\n"
"         connection_flights AS \"Connections\",\n"
"         flight_1_departure AS \"Departure1\",\n"
"         flight_1_arrival AS \"Arrival1\",\n"
"         flight_2_departure AS \"Departure2\",\n"
"         flight_2_arrival AS \"Arrival2\",\n"
"         time_elapsed AS \"Duration\",\n"
"         f1_aircraft AS \"Aircraft1\",\n"
"         f2_aircraft AS \"Aircraft2\",\n"
"         flight_id2 AS \"Flight2\"\n"
"FROM     total_flights\n"
"WHERE    n_seats != 0\n"
"ORDER BY time_elapsed, flight_1_departure;";


  if (!(f = fopen("salida.txt", "w"))) return;

  ret = odbc_connect(&env, &dbc);
  if(!SQL_SUCCEEDED(ret)) {
    return;
  }

  ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    return;
  }

  ret = SQLPrepare(stmt, (SQLCHAR *) query, SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return;
  }

  fprintf(f, "%s\n", query);

  ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(from), 0, from, strlen(from)+1, NULL);
  if (!SQL_SUCCEEDED(ret)) {
    odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return;
  }

/*********************************************          BIND PARAMETERS         ****************************************************************+*/
  
  ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(to), 0, to, strlen(to)+1, NULL);
  if (!SQL_SUCCEEDED(ret)) {
    odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return;
  }
  
  ret = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(date), 0, date, strlen(date)+1, NULL);
  if (!SQL_SUCCEEDED(ret)) {
    odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return;
  }
  
  ret = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(from), 0, from, strlen(from)+1, NULL);
  if (!SQL_SUCCEEDED(ret)) {
    odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return;
  }
  
  ret = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(to), 0, to, strlen(to)+1, NULL);
  if (!SQL_SUCCEEDED(ret)) {
    odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return;
  }
  
  ret = SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(date), 0, date, strlen(date)+1, NULL);
  if (!SQL_SUCCEEDED(ret)) {
    odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return;
  }


/*********************************************             EXECUTE            ****************************************************************+*/


  ret = SQLExecute(stmt);
  if (!SQL_SUCCEEDED(ret)) {
      odbc_extract_error("salida.txt", stmt, SQL_HANDLE_STMT);
      return;
  }



/*********************************************         BIND EXIT COLUMNS         ****************************************************************+*/


  ret = SQLBindCol(stmt, 1, SQL_C_LONG, &flight_id, sizeof(flight_id), &len1);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para flight_id.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 2, SQL_C_LONG, &number_of_seats, sizeof(number_of_seats), &len2);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para number_of_seats.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 3, SQL_C_LONG, &connection_flights, sizeof(connection_flights), &len3);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para connection_flights.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 4, SQL_C_CHAR, f1_departure_date, sizeof(f1_departure_date), &len4);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para f1_departure_date.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 5, SQL_C_CHAR, f1_arrival_date, sizeof(f1_arrival_date), &len5);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para f1_arrival_date.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 6, SQL_C_CHAR, f2_departure_date, sizeof(f2_departure_date), &len6);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para f2_departure_date.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 7, SQL_C_CHAR, f2_arrival_date, sizeof(f2_arrival_date), &len7);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para f2_arrival_date.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 8, SQL_C_CHAR, time_elapsed, sizeof(time_elapsed), &len8);    
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para time_elapsed.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 9, SQL_C_CHAR, f1_aircraft_code, sizeof(f1_aircraft_code), &len9);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para f1_aircraft_code.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }
  
  ret = SQLBindCol(stmt, 10, SQL_C_CHAR, f2_aircraft_code, sizeof(f2_aircraft_code), &len10);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para f2_aircraft_code.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }

  ret = SQLBindCol(stmt, 11, SQL_C_LONG, &flight_id2, sizeof(flight_id2), &len11);
  if (!SQL_SUCCEEDED(ret)) {
    fprintf(stderr, "No se pudo hacer BindCol para flight_id2.\n");
    odbc_extract_error("SQLBindCol", stmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    odbc_disconnect(env, dbc);
    return;
  }

/**********************************************                 READ QUERY RESULTS                        **************************************************+*/

  sprintf(header, "%-15s %-15s %-15s %-20s %-20s %-15s\n",
    "Flight", "Seat", "Connections", "Departure", "Arrival", "Duration");

  /* Guardar el encabezado como primera fila del menú */
  strncpy((*choices)[row], header, strlen(header) + 1);
  (*choices)[row][strlen(header)] = '\0';
  strncpy((*choices_extra)[row], "", strlen("") + 1);
  (*choices_extra)[row][strlen("")] = '\0';
  row++;

  /* importante que el tope del bucle sea MAX_RESULTS para guardar más de una página */
  while (SQL_SUCCEEDED(SQLFetch(stmt)) && row < MAX_RESULTS) 
  { 
    
    /* default values in case of null attributes */
    if (len4 == SQL_NULL_DATA) f1_departure_date[0] = '\0'; 
    if (len5 == SQL_NULL_DATA) f1_arrival_date[0] = '\0';
    if (len6 == SQL_NULL_DATA) f2_departure_date[0] = '\0';
    if (len7 == SQL_NULL_DATA) f2_arrival_date[0] = '\0';
    if (len8 == SQL_NULL_DATA) time_elapsed[0] = '\0';
    if (len9 == SQL_NULL_DATA) f1_aircraft_code[0] = '\0';
    if (len10 == SQL_NULL_DATA) f2_aircraft_code[0] = '\0';
    if (len11 == SQL_NULL_DATA) flight_id2 = -1;    

    

    if (flight_id2 == -1) {
      sprintf(buf1, "%-15d %-15d %-15d %-20s %-20s %-15s", flight_id, number_of_seats, connection_flights, f1_departure_date, f1_arrival_date, time_elapsed);
      sprintf(buf2, "%-7d %-20s %-20s %-15s", flight_id, f1_departure_date, f1_arrival_date, f1_aircraft_code);
    } else {
      sprintf(buf1, "%-15d %-15d %-15d %-20s %-20s %-15s", flight_id, number_of_seats, connection_flights, f1_departure_date, f2_arrival_date, time_elapsed);
      sprintf(buf2, "%-7d %-7d %-20s %-20s %-20s %-20s %-10s %-10s", flight_id, flight_id2, f1_departure_date, f1_arrival_date, f2_departure_date, f2_arrival_date, f1_aircraft_code, f2_aircraft_code);
    }

    t1 = strlen(buf1)+1;
    t1 = MIN(t1, MAX_TUPLE_LENGTH);

    t2 = strlen(buf2)+1;
    t2 = MIN(t2, MAX_MESSAGE_LENGTH);

    /* copy up to max_length-1 characters, ensure NUL termination */
    strncpy((*choices)[row], (char*)buf1, t1);
    (*choices)[row][t1 - 1] = '\0';
    strncpy((*choices_extra)[row], (char*)buf2, t2);
    (*choices_extra)[row][t2 - 1] = '\0';
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
