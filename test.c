#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "duckdb.h"

void display();
void selectFromDoc(duckdb_connection con);
duckdb_state insertIntoDoc(duckdb_prepared_statement stmt, char *docId, char *text);


duckdb_state insertIntoDoc(duckdb_prepared_statement stmt, char *docId, char *text) {
    duckdb_bind_varchar(stmt, 1, docId); // the parameter index starts counting at 1!
    duckdb_bind_varchar(stmt, 2, text);

    return duckdb_execute_prepared(stmt, NULL);
}

void selectFromDoc(duckdb_connection con) {
    duckdb_state state;
    duckdb_result result;

    state = duckdb_query(con, "SELECT * FROM test", &result);
    if (state == DuckDBError) {
        // handle error
        printf("select error\n");
        const char *msg = duckdb_result_error(&result);
        printf("%s\n", msg);
    }

    duckdb_data_chunk res = duckdb_fetch_chunk(result);

    // get the number of rows from the data chunk
    idx_t row_count = duckdb_data_chunk_get_size(res);
    printf("got %lu rows\n", row_count);

    duckdb_vector res_col_1 = duckdb_data_chunk_get_vector(res, 0);
    duckdb_string_t *vector_data_1 = (duckdb_string_t *) duckdb_vector_get_data(res_col_1);
    uint64_t *vector_validity_1 = duckdb_vector_get_validity(res_col_1);
    duckdb_vector res_col_2 = duckdb_data_chunk_get_vector(res, 1);
    duckdb_string_t *vector_data_2 = (duckdb_string_t *) duckdb_vector_get_data(res_col_2);
    uint64_t *vector_validity_2 = duckdb_vector_get_validity(res_col_2);
    for (idx_t row = 0; row < row_count; row++) {
		if (duckdb_validity_row_is_valid(vector_validity_1, row)) {
			duckdb_string_t str = vector_data_1[row];
			if (duckdb_string_is_inlined(str)) {
				// use inlined string
				printf("%.*s", str.value.inlined.length, str.value.inlined.inlined);
			} else {
				// follow string pointer
				printf("%.*s", str.value.pointer.length, str.value.pointer.ptr);
			}
		} else {
			printf("NULL");
		}
        printf(", ");
        if (duckdb_validity_row_is_valid(vector_validity_2, row)) {
			duckdb_string_t str2 = vector_data_2[row];
			if (duckdb_string_is_inlined(str2)) {
				// use inlined string
				printf("%.*s", str2.value.inlined.length, str2.value.inlined.inlined);
			} else {
				// follow string pointer
				printf("%.*s", str2.value.pointer.length, str2.value.pointer.ptr);
			}
		} else {
			printf("NULL");
		}
        printf("\n");
	}

    // destroy the result after we are done with it
    duckdb_destroy_result(&result);
}

void display() {
    
}

int main() {
    char input[100];
    char title[100];
    char text[1000];

    duckdb_database db;
    duckdb_connection con;

    const char path[100] = "/home/luwei/code/duckdb/build/release/documents.duckdb";
    // char msg[1000];

    if (duckdb_open(path, &db) == DuckDBError) {
        // handle error
        printf("open error");
        return 1;
    }
    if (duckdb_connect(db, &con) == DuckDBError) {
        // handle error
        printf("connect error");
        return 1;
    }

    duckdb_prepared_statement stmt;
    if (duckdb_prepare(con, "insert into test values($1, $2)", &stmt) == DuckDBError) {
        // handle error
        printf("prepare error");
        return 1;
    }

    while (true) {
        printf("请选择：\n");
        printf("1\\ insert\n");
        printf("2\\ select\n");

        fgets(input, sizeof(input), stdin);
        size_t len = strlen(input);
        if (len > 0 && input[len - 1] == '\n') {
            input[len - 1] = '\0';
        }
        // printf("%s\n", input);
        // break;
        if (strcmp(input, "select") == 0) {
            // printf("got select\n");
            selectFromDoc(con);
        }

        if (strcmp(input, "insert") == 0) {
            // printf("got insert\n");
            printf("输入标题：");
            fgets(title, sizeof(title), stdin);
            len = strlen(title);
            if (len > 0 && title[len - 1] == '\n') {
                title[len - 1] = '\0';
            }
            // printf("got %s\n", title);
            printf("输入内容：");
            fgets(text, sizeof(text), stdin);
            len = strlen(text);
            if (len > 0 && text[len - 1] == '\n') {
                text[len - 1] = '\0';
            }
            // printf("got %s\n", text);
            insertIntoDoc(stmt, title, text);

        }

        if (strcmp(input, ".q") == 0) {
            printf("Bye\n");
            break;
        }
    }

    duckdb_destroy_prepare(&stmt);
    // cleanup
    duckdb_disconnect(&con);
    duckdb_close(&db);

    return 0;
}