#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "duckdb.h"

#ifdef _WIN32
#include <io.h> // Windows 下的 access 函数
#define access _access
#define F_OK 0
#else
#include <unistd.h> // 类 Unix 系统
#endif

#define SHOW_CODE 0
#define APPEND_CODE 1
#define QUIT_CODE -100
#define ERROR_CODE -1

#define MSG_LENGTH 1024

int display();
void selectFromDoc(duckdb_connection con);
void insertIntoDoc(duckdb_prepared_statement stmt);


void insertIntoDoc(duckdb_prepared_statement stmt) {
    char title[100];
    char content[10000];
    duckdb_state state;

    fprintf(stdout, "title: ");
    fgets(title, sizeof(title), stdin);
    size_t len = strlen(title);
    if (len > 0 && title[len - 1] == '\n') {
        title[len - 1] = '\0';
    }
    fprintf(stdout, "content: ");
    fgets(content, sizeof(content), stdin);
    len = strlen(content);
    if (len > 0 && content[len - 1] == '\n') {
        content[len - 1] = '\0';
    }

    duckdb_bind_varchar(stmt, 2, title); // the parameter index starts counting at 1!
    duckdb_bind_varchar(stmt, 3, content);

    state = duckdb_execute_prepared(stmt, NULL);
    if (state == DuckDBSuccess) {
        printf("append success\n");
    } else {
        printf("code: %d\n", state);
        const char *msg = duckdb_prepare_error(stmt);
        if (msg == NULL) {
            printf("got null");
        }
    }
}

void selectFromDoc(duckdb_connection con) {
    duckdb_state state;
    duckdb_result result;

    state = duckdb_query(con, "SELECT title, content FROM documents", &result);
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

int display() {
    fprintf(stdout,
    "******************************\n"
    "  --show       输出表中内容\n"
    "  --append     插入文本\n"
    "  --quit       退出程序\n"
    "******************************\n"
    "--"
    );

    char input[100];
    fgets(input, sizeof(input), stdin);
    // printf("get %s\n", input);
    size_t len = strlen(input);
    if (len > 0 && input[len - 1] == '\n') {
        input[len - 1] = '\0';
    }
    if (strcmp(input, "show") == 0) {
        return SHOW_CODE;
    } else if (strcmp(input, "append") == 0) {
        return APPEND_CODE;
    } else if (strcmp(input, "quit") == 0) {
        return QUIT_CODE;
    } else {
        return ERROR_CODE;
    }
}

void createTable(const char *path) {
    duckdb_database db;
    char *errMsg[1000];
    duckdb_state state;
    duckdb_connection con;

    if (duckdb_open_ext(path, &db, NULL, errMsg) == DuckDBSuccess) {
        printf("创建数据库成功\n");

        if (duckdb_connect(db, &con) == DuckDBError) {
            // handle error
            printf("connect error");
        }

        state = duckdb_query(con, "CREATE TABLE documents (docId INT, title VARCHAR, content VARCHAR);", NULL);
        if (state == DuckDBSuccess) {
            printf("创建数据表成功\n");
        } else {
            printf("创建数据表失败\n");
        }

        duckdb_disconnect(&con);
        duckdb_close(&db);
    } else {
        printf("create table error: %s\n", errMsg[0]);
    }
}

int main() {
    int code;
    char title[100] = "title test";
    char text[1000] = "content test";
    char *errMsg[100];

    duckdb_database db;
    duckdb_connection con;

    const char path[100] = "./storage/documents.duckdb";
    // char msg[1000];

    if (access(path, F_OK) != 0) {
        createTable(path);
        exit(0);
    }

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
    if (duckdb_prepare(con, "insert into documents values($1, $2, $3)", &stmt) == DuckDBError) {
        // handle error
        printf("prepare error");
        return 1;
    }

    while (true) {
        code = display();
        switch (code)
        {
            case QUIT_CODE:
                printf("Bye\n");
                exit(0);
                break;
            case ERROR_CODE:
                printf("选项输入错误，请重新输入。\n");
                break;
            case SHOW_CODE:
                selectFromDoc(con);
                break;
            case APPEND_CODE:
                insertIntoDoc(stmt);
                break;
            default:
                break;
        }
    }

    duckdb_destroy_prepare(&stmt);
    // cleanup
    duckdb_disconnect(&con);
    duckdb_close(&db);

    return 0;
}