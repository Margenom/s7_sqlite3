/* Object file
 * cc -fpic -I<here s7 headers> -c db.c -o db.o
 * Liblary
 * cc -shared -Wl,-soname,libdb.so -o libdb.so db.o -L<here s7 liblary> -ls7 -lsqlite3 -lc -lpthread
 */

#include <sqlite3.h>
#include <stddef.h>
#include <string.h>
#include <stdint.h>
#include <s7.h>

static s7_pointer db_open(s7_scheme *sc, s7_pointer args) {
	sqlite3 *pdb;
	if (sqlite3_open(s7_string(s7_car(args)), &pdb)) {
		sqlite3_close(pdb);
		return s7_make_boolean(sc, 0);	
	}
	return s7_make_c_pointer(sc, pdb); 
}
static s7_pointer db_close(s7_scheme *sc, s7_pointer args) {
	sqlite3_close(s7_c_pointer(s7_car(args)));
	return s7_unspecified(sc);
}
static s7_pointer db_exec(s7_scheme *sc, s7_pointer args) {
	char *ErrS;const char *Tail;
	sqlite3 *db = s7_c_pointer(s7_car(args));
	const char *sql = s7_string(s7_cadr(args));
	sqlite3_stmt *st;

	int rst= sqlite3_prepare_v2(db, sql, strlen(sql), &st, &Tail);
	if (SQLITE_OK != rst)
		//return s7_make_integer(sc, rst);
		return s7_make_string(sc, sqlite3_errmsg(db));

	s7_pointer Pams = s7_cddr(args);
	int pami = 1;
	while (!s7_is_null(sc, Pams)) {
		s7_pointer Param = s7_car(Pams);
		if (s7_is_integer(Param)) sqlite3_bind_int64(st, pami, s7_integer(Param)); else
		if (s7_is_real(Param)) sqlite3_bind_double(st, pami, s7_real(Param)); else
		if (s7_is_string(Param)) sqlite3_bind_text(st, pami, s7_string(Param), 
			s7_string_length(Param), SQLITE_TRANSIENT);
		else sqlite3_bind_null(st, pami);
		pami++; Pams = s7_cdr(Pams);
	}
	rst = sqlite3_step(st);
	s7_pointer Ret;
	switch (rst) {
		case SQLITE_DONE: Ret = s7_unspecified(sc); goto db_exec_out;
		case SQLITE_ERROR: Ret = s7_make_string(sc, sqlite3_errmsg(db)); goto db_exec_err;
		default: Ret = s7_nil(sc);
	}
	while (rst == SQLITE_ROW) {
		int count = sqlite3_column_count(st);
		s7_pointer Row = s7_make_vector(sc, count);
		for (int i=0; i<count; ++i) {
			s7_pointer Atm;
			switch (sqlite3_column_type(st, i)) {
				case SQLITE_INTEGER: Atm = s7_make_integer(sc, (s7_int) sqlite3_column_int64(st, i)); break;
				case SQLITE_FLOAT: Atm = s7_make_real(sc, sqlite3_column_double(st, i)); break;
				case SQLITE_TEXT: Atm = s7_make_string(sc, sqlite3_column_text(st, i)); break;
				default: Atm = s7_nil(sc);
			}
			s7_vector_set(sc, Row, i, Atm);
		}
		Ret = s7_cons(sc, Row, Ret);
		rst = sqlite3_step(st);
	}	
	Ret = s7_reverse(sc, Ret);
db_exec_out:	
db_exec_err:	
	sqlite3_finalize(st);
	return Ret;
}

void db_init(s7_scheme *sc);
void db_init(s7_scheme *sc) {
	s7_define_function(sc, "db-open", db_open, 1, 0, 0, "Open Sqlite3 DataBase");
	s7_define_function(sc, "db-close", db_close, 1, 0, 0, "Close Sqlite3 DataBase");
	s7_define_function(sc, "db-exec", db_exec, 2, 0, 1, "Execute sql-request Sqlite3 DataBase");
}
