# FUNDB Práctica 2
# Grupo: Guilherme Povedano y Marco Mancennido 
# 


########################################################################################

CC = gcc -g
CFLAGS = -Wall -Wextra -pedantic -ansi
LDLIBS = -lodbc -lcurses -lpanel -lmenu -lform
EXE = menu
OBJ = $(EXE).o bpass.o loop.o  search.o windows.o

########################################################################################

export PGDATABASE:=flight
export PGUSER :=alumnodb
export PGPASSWORD :=alumnodb
export PGCLIENTENCODING:=UTF8
export PGHOST:=localhost

DBNAME =$(PGDATABASE)
PSQL = psql
CREATEDB = createdb
DROPDB = dropdb --if-exists
PG_DUMP = pg_dump
PG_RESTORE = pg_restore

########################################################################################

all: $(EXE)


%.o: %.c $(HEADERS)
	@echo Compiling $<...
	$(CC) $(CFLAGS) -c -o $@ $<

# link binary
$(EXE): $(DEPS) $(OBJ)
	$(CC) -o $(EXE) $(OBJ) $(LDLIBS)

########################################################################################

db: dropdb createdb restore shell

createdb:
	@echo Creando BBDD
	@$(CREATEDB)
dropdb:
	@echo Eliminando BBDD
	@$(DROPDB) $(DBNAME)
	rm -f *.log
dump:
	@echo creando dumpfile
	@$(PG_DUMP) > $(DBNAME).sql
restore:
	@echo restore data base
	@cat $(DBNAME).sql | $(PSQL)  
psql: shell
shell:
	@echo create psql shell
	@$(PSQL)  

########################################################################################

clean :
	rm -f *.o core $(EXE)

run:
	./$(EXE)

valgrind:
	valgrind --leak-check=full ./$(EXE)
