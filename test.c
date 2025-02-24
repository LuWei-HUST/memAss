#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "duckdb.h"
#include "lib/jieba.h"

#ifdef _WIN32
#include <io.h> // Windows 下的 access 函数
#define access _access
#define F_OK 0
#else
#include <unistd.h> // 类 Unix 系统
#endif

#define SHOW_CODE 0
#define APPEND_CODE 1
#define INDEX_CODE 2
#define SELECT_CODE 3
#define QUIT_CODE -100
#define ERROR_CODE -1

#define MSG_LENGTH 1024
#define MAX_LINES 2000
#define MAX_WORD 1000
#define MAX_QUERY 2048

const char* DICT_PATH = "./dict/jieba.dict.utf8";
const char* HMM_PATH = "./dict/hmm_model.utf8";
const char* USER_DICT = "./dict/user.dict.utf8";
const char* IDF_PATH = "./dict/idf.utf8";
const char* STOP_WORDS_PATH = "./dict/stop_words.utf8";

typedef struct {
    char *key;
    int *indexes;
    int indexLen;
} indexMap;

typedef struct indexCount {
    int index;
    int count;
    struct indexCount *next;
} indexCount;

void selectFromDoc(duckdb_connection con);
void insertIntoDoc(duckdb_prepared_statement stmt);
void buildIndex(Jieba handle, const char *path, duckdb_connection con, char *col);
void selectByKey(Jieba handle, duckdb_connection con, char *input);
void createTable(const char *path);
void loadStopWords(duckdb_connection con, const char *path);
char **tokenize(Jieba handle, char *text, char *lines[], int line_count, int *filteredWordsLen);
void inverted_index(indexMap *invertedIndex, int *invertedIndexLen, char **filteredWords, int filteredWordsLen, int docId);
void deleteItem(duckdb_connection con, char *title);


void insertIntoDoc(duckdb_prepared_statement stmt) {
    char title[256];
    char content[10240];
    duckdb_state state;

    fprintf(stdout, "title: ");
    fgets(title, sizeof(title), stdin);
    size_t len = strlen(title);
    
    if (len >= 255) {
        fprintf(stderr, "标题太长啦，最多254个字符。\n");
        char c;
        while (c=getchar()!='\n' && c!=EOF) {};
        return ;
    }

    if (len > 0 && title[len - 1] == '\n') {
        title[len - 1] = '\0';
    }
    fprintf(stdout, "content: ");
    fgets(content, sizeof(content), stdin);
    len = strlen(content);

    if (len >= 10000) {
        fprintf(stderr, "内容太长啦，最多10000个字符。\n");
        char c;
        while (c=getchar()!='\n' && c!=EOF) {};
        return ;
    }

    if (len > 0 && content[len - 1] == '\n') {
        content[len - 1] = '\0';
    }

    duckdb_bind_varchar(stmt, 1, title); // the parameter index starts counting at 1!
    duckdb_bind_varchar(stmt, 2, content);

    state = duckdb_execute_prepared(stmt, NULL);
    if (state == DuckDBSuccess) {
        printf("append success\n");
    } else {
        printf("append failed\n");
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
        return ;
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

void selectByKey(Jieba handle, duckdb_connection con, char *input) {
    char *querytext = malloc(MAX_WORD);

    strcpy(querytext, input+7);

    if (strlen(querytext) == 0) {
        fprintf(stdout, "input nothing\n");
        return ;
    }

    // fprintf(stdout, "select %s\n", keyword);
    // 把停用词读进来
    // printf("加载停用词\n");
    FILE *fin;
    fin = fopen(STOP_WORDS_PATH, "r");

    if(fin == NULL) {
        printf("停用词文件打开失败\n");
        return ;
    }

    char buffer[256];
    char *lines[MAX_LINES];
    int line_count = 0;

    while (fgets(buffer, sizeof(buffer), fin) != NULL) {
        // 去掉行末的换行符（如果有）
        buffer[strcspn(buffer, "\n")] = '\0';

        // 为当前行分配内存
        lines[line_count] = malloc(strlen(buffer) + 1);  // +1 用于存储 '\0'
        if (lines[line_count] == NULL) {
            perror("内存分配失败");
            fclose(fin);
            return ;
        }

        // 将缓冲区的内容复制到字符串数组中
        strcpy(lines[line_count], buffer);

        // 增加行数
        line_count++;

        // 如果行数超过数组容量，退出
        if (line_count >= MAX_LINES) {
            printf("已达到最大行数限制\n");
            break;
        }
    }

    fclose(fin);

    char **queryWords;
    int queryWordsLen = 0;
    queryWords = tokenize(handle, querytext, lines, line_count, &queryWordsLen);

    // fprintf(stdout, "分词后的查询文本：\n");
    // for (int i=0;i<queryWordsLen;i++) {
    //     fprintf(stdout, "%s\n", queryWords[i]);
    // }
    // fprintf(stdout, "\n");

    duckdb_state state;
    duckdb_result result;

    char *querysql = malloc(MAX_QUERY);
    char *str2 = malloc(MAX_QUERY);

    char str1[] = "select * from documents where docId in (select distinct(documents.docId) from document_content_fts_index, documents where words in ";
    char str3[] = " and document_content_fts_index.docId=documents.docId);";

    strcpy(str2, "[");
    if (queryWordsLen) {
        for (int i=0;i<queryWordsLen;i++) {
            if (i == queryWordsLen - 1) {
                strcat(str2, "'");
                strcat(str2, queryWords[i]);
                strcat(str2, "']");
            } else {
                strcat(str2, "'");
                strcat(str2, queryWords[i]);
                strcat(str2, "', ");
            }
        }
    } else {
        fprintf(stdout, "meaningless words to select\n");
        return ;
    }

    strcpy(querysql, str1);
    strcat(querysql, str2);
    strcat(querysql, str3);

    // fprintf(stdout, "%s\n", querysql);

    state = duckdb_query(con, querysql, &result);
    if (state == DuckDBSuccess) {
        duckdb_data_chunk res = duckdb_fetch_chunk(result);

        // get the number of rows from the data chunk
        idx_t row_count = duckdb_data_chunk_get_size(res);
        printf("got %lu rows\n", row_count);

        duckdb_vector col1 = duckdb_data_chunk_get_vector(res, 0);
        int32_t *col1_data = (int32_t *) duckdb_vector_get_data(col1);
        uint64_t *col1_validity = duckdb_vector_get_validity(col1);
        duckdb_vector res_col_2 = duckdb_data_chunk_get_vector(res, 1);
        duckdb_string_t *vector_data_2 = (duckdb_string_t *) duckdb_vector_get_data(res_col_2);
        uint64_t *vector_validity_2 = duckdb_vector_get_validity(res_col_2);
        duckdb_vector res_col_3 = duckdb_data_chunk_get_vector(res, 2);
        duckdb_string_t *vector_data_3 = (duckdb_string_t *) duckdb_vector_get_data(res_col_3);
        uint64_t *vector_validity_3 = duckdb_vector_get_validity(res_col_3);
        for (idx_t row = 0; row < row_count; row++) {
            if (duckdb_validity_row_is_valid(col1_validity, row)) {
                printf("%d", col1_data[row]);
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
            printf(", ");
            if (duckdb_validity_row_is_valid(vector_validity_3, row)) {
                duckdb_string_t str3 = vector_data_3[row];
                if (duckdb_string_is_inlined(str3)) {
                    // use inlined string
                    printf("%.*s", str3.value.inlined.length, str3.value.inlined.inlined);
                } else {
                    // follow string pointer
                    printf("%.*s", str3.value.pointer.length, str3.value.pointer.ptr);
                }
            } else {
                printf("NULL");
            }
            printf("\n");
        }

        // destroy the result after we are done with it
        duckdb_destroy_result(&result);
    } else {
        fprintf(stderr, "通过倒排索引查询失败\n");
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
            printf("connect error\n");
        }

        state = duckdb_query(con, "CREATE SEQUENCE doc_id_seq START 1;CREATE TABLE documents (docId INT PRIMARY KEY DEFAULT nextval('doc_id_seq'), title VARCHAR, content VARCHAR);", NULL);
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

void loadStopWords(duckdb_connection con, const char *path) {
    duckdb_result result;
    // 查询停用词表
    duckdb_state state = duckdb_query(con, "SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_name = 'stopwords');", &result);
    if (state == DuckDBError) {
        // handle error
        printf("select error\n");
        const char *msg = duckdb_result_error(&result);
        printf("%s\n", msg);
    }
    duckdb_data_chunk res = duckdb_fetch_chunk(result);
    duckdb_vector col1 = duckdb_data_chunk_get_vector(res, 0);
    char *col1_data = (char *) duckdb_vector_get_data(col1);
    uint64_t *col1_validity = duckdb_vector_get_validity(col1);

    if (col1_validity[0]) {
        // printf("got %d\n", col1_data[0]);
        if (col1_data[0] == 0) {    // 不存在则创建
            state = duckdb_query(con, "CREATE TABLE stopwords(words VARCHAR);", NULL);
            if (state == DuckDBSuccess) {
                printf("停用词表创建成功\n");
                // 从文件中读取
                printf("加载停用词\n");
                FILE *fin;
                fin = fopen(path, "r");

                if(fin == NULL) {
                    printf("停用词文件打开失败\n");
                    return ;
                }

                char buffer[256];
                char *lines[MAX_LINES];
                int line_count = 0;

                while (fgets(buffer, sizeof(buffer), fin) != NULL) {
                    // 去掉行末的换行符（如果有）
                    buffer[strcspn(buffer, "\n")] = '\0';

                    // 为当前行分配内存
                    lines[line_count] = malloc(strlen(buffer) + 1);  // +1 用于存储 '\0'
                    if (lines[line_count] == NULL) {
                        perror("内存分配失败");
                        fclose(fin);
                        return ;
                    }

                    // 将缓冲区的内容复制到字符串数组中
                    strcpy(lines[line_count], buffer);

                    // 增加行数
                    line_count++;

                    // 如果行数超过数组容量，退出
                    if (line_count >= MAX_LINES) {
                        printf("已达到最大行数限制\n");
                        break;
                    }
                }

                fclose(fin);

                duckdb_appender appender;
                if (duckdb_appender_create(con, NULL, "stopwords", &appender) == DuckDBError) {
                // handle error
                }
                for (int i=0;i<line_count;i++) {
                    duckdb_append_varchar(appender, lines[i]);
                    duckdb_appender_end_row(appender);
                }

                duckdb_appender_destroy(&appender);
            } else {
                printf("停用词表创建失败\n");
            }
        }
    } else {
        printf("查询停用词表是否存在时，返回值无效\n");
    }
}

// 分词并过滤停用词
char **tokenize(Jieba handle, char *text, char *lines[], int line_count, int *filteredWordsLen) {
    // printf("tokenize %s\n", text);
    size_t len = strlen(text);
    CJiebaWord* x;
    CJiebaWord* words = Cut(handle, text, len);

    int flag;
    char **filteredWords = malloc(MAX_WORD * sizeof(char *));
    char buffer[MAX_WORD];

    int count = 0;
    for (x = words; x->word; x++) {
        // printf("%*.*s\n", x->len, x->len, x->word);
        flag = 0;
        for (int i=0;i<line_count;i++) {
            if (strncmp(x->word, lines[i], x->len) == 0) {
                // printf("stop word: %*.*s\n", x->len, x->len, x->word);
                flag = 1;
                break;
            }
        }

        if (!flag) {
            // if (count >= MAX_WORD) {
            //     // printf("重新分配内存\n");
            //     char **filteredWordsNew = malloc((MAX_WORD+count) * sizeof(char *));;
            //     memcpy(filteredWordsNew, filteredWords, count*sizeof(char *));
            //     free(filteredWords);
            //     filteredWords = filteredWordsNew;
            // }

            filteredWords[count] = malloc(sizeof(buffer) + 1);
            if (filteredWords[count] == NULL) {
                perror("内存分配失败");
                return NULL;
            }

            strncpy(filteredWords[count], x->word, x->len);
            filteredWords[count][x->len] = '\0';
            
            count++;
        }
    }

    *filteredWordsLen = count;
    // printf("count:%d\n", count);

    // for (int i=0;i<count;i++) {
    //     fprintf(stdout, "%s\n", filteredWords[i]);
    // }

    return filteredWords;
}

void inverted_index(indexMap *invertedIndex, int *invertedIndexLen, char **filteredWords, int filteredWordsLen, int docId) {
    // printf("create inverted index\n");
    int existsFlag = 0;
    int n = *invertedIndexLen;
    int *tempIndex;
    
    for (int i=0;i<filteredWordsLen;i++) {
        
        existsFlag = 0;
        for (int j=0;j<n;j++) {
            if(strcmp(invertedIndex[j].key, filteredWords[i])==0) {
                existsFlag = 1;

                tempIndex = malloc(sizeof(int)*(invertedIndex[j].indexLen+1));
                memcpy(tempIndex, invertedIndex[j].indexes, sizeof(int)*invertedIndex[j].indexLen);
                free(invertedIndex[j].indexes);
                tempIndex[invertedIndex[j].indexLen] = docId;
                invertedIndex[j].indexes = tempIndex;
                invertedIndex[j].indexLen += 1;

                break;
            }
        }

        if (existsFlag == 0) {
            // fprintf(stdout, "filteredWords[i]: %s\n", filteredWords[i]);
            invertedIndex[n].key = malloc(strlen(filteredWords[i])+1);
            strcpy(invertedIndex[n].key, filteredWords[i]);

            invertedIndex[n].indexes = malloc(sizeof(int));
            invertedIndex[n].indexes[0] = docId;
            invertedIndex[n].indexLen = 1;

            n += 1;
        }
    }

    *invertedIndexLen = n;
}

void buildIndex(Jieba handle, const char *path, duckdb_connection con, char *col) {
    // 把停用词读进来
    // printf("加载停用词\n");
    FILE *fin;
    fin = fopen(path, "r");

    if(fin == NULL) {
        printf("停用词文件打开失败\n");
        return ;
    }

    char buffer[256];
    char *lines[MAX_LINES];
    int line_count = 0;

    while (fgets(buffer, sizeof(buffer), fin) != NULL) {
        // 去掉行末的换行符（如果有）
        buffer[strcspn(buffer, "\n")] = '\0';

        // 为当前行分配内存
        lines[line_count] = malloc(strlen(buffer) + 1);  // +1 用于存储 '\0'
        if (lines[line_count] == NULL) {
            perror("内存分配失败");
            fclose(fin);
            return ;
        }

        // 将缓冲区的内容复制到字符串数组中
        strcpy(lines[line_count], buffer);

        // 增加行数
        line_count++;

        // 如果行数超过数组容量，退出
        if (line_count >= MAX_LINES) {
            printf("已达到最大行数限制\n");
            break;
        }
    }

    fclose(fin);

    // 把要建立索引的那一列读进来
    duckdb_state state;
    duckdb_result result;

    state = duckdb_query(con, "SELECT docId, title, content FROM documents", &result);
    if (state == DuckDBError) {
        // handle error
        printf("select error\n");
        const char *msg = duckdb_result_error(&result);
        printf("%s\n", msg);
    }

    duckdb_data_chunk res = duckdb_fetch_chunk(result);

    // get the number of rows from the data chunk
    idx_t row_count = duckdb_data_chunk_get_size(res);
    // printf("got %lu rows\n", row_count);

    duckdb_vector col1 = duckdb_data_chunk_get_vector(res, 0);
    int32_t *col1_data = (int32_t *) duckdb_vector_get_data(col1);
    uint64_t *col1_validity = duckdb_vector_get_validity(col1);
    duckdb_vector res_col_1 = duckdb_data_chunk_get_vector(res, 1);
    duckdb_string_t *vector_data_1 = (duckdb_string_t *) duckdb_vector_get_data(res_col_1);
    uint64_t *vector_validity_1 = duckdb_vector_get_validity(res_col_1);
    duckdb_vector res_col_2 = duckdb_data_chunk_get_vector(res, 2);
    duckdb_string_t *vector_data_2 = (duckdb_string_t *) duckdb_vector_get_data(res_col_2);
    uint64_t *vector_validity_2 = duckdb_vector_get_validity(res_col_2);
    // for (idx_t row = 0; row < row_count; row++) {
	// 	if (duckdb_validity_row_is_valid(vector_validity_1, row)) {
	// 		duckdb_string_t str = vector_data_1[row];
	// 		if (duckdb_string_is_inlined(str)) {
	// 			// use inlined string
	// 			printf("%.*s", str.value.inlined.length, str.value.inlined.inlined);
	// 		} else {
	// 			// follow string pointer
	// 			printf("%.*s", str.value.pointer.length, str.value.pointer.ptr);
	// 		}
	// 	} else {
	// 		printf("NULL");
	// 	}
    //     printf("\n");
	// }

    char **filteredWords;
    int filteredWordsLen;
    indexMap *invertedIndex = malloc(sizeof(indexMap)*MAX_WORD);
    int invertedIndexLen = 0;

    char *tempChar = malloc(MAX_WORD);

    for (int i=0;i<row_count;i++) {
        if (!duckdb_validity_row_is_valid(col1_validity, i) || !duckdb_validity_row_is_valid(vector_validity_1, i) || !duckdb_validity_row_is_valid(vector_validity_2, i)) {
            fprintf(stderr, "data invalid\n");
            continue;
        }

        duckdb_string_t str = vector_data_2[i];
        duckdb_string_t str1 = vector_data_1[i];
        if (duckdb_string_is_inlined(str)) {
            // use inlined string
            strncpy(tempChar, str.value.inlined.inlined, MAX_WORD);
            strcat(tempChar, " ");
            if (duckdb_string_is_inlined(str1)) {
                // use inlined string
                strncat(tempChar, str1.value.inlined.inlined, str1.value.inlined.length);
            } else {
                // follow string pointer
                strncat(tempChar, str1.value.pointer.ptr, str1.value.pointer.length);
            }
        } else {
            // follow string pointer
            strncpy(tempChar, str.value.pointer.ptr, str.value.pointer.length);
            tempChar[str.value.pointer.length] = '\0';
            strcat(tempChar, " ");
            if (duckdb_string_is_inlined(str1)) {
                // use inlined string
                strncat(tempChar, str1.value.inlined.inlined, str1.value.inlined.length);
            } else {
                // follow string pointer
                strncat(tempChar, str1.value.pointer.ptr, str1.value.pointer.length);
            }
        }
        // fprintf(stdout, "tempChar: %s\n", tempChar);

        filteredWords = tokenize(handle, tempChar, lines, line_count, &filteredWordsLen);
        // printf("done\n");

        inverted_index(invertedIndex, &invertedIndexLen, filteredWords, filteredWordsLen, col1_data[i]);
    }

    // for (int i=0;i<invertedIndexLen;i++) {
    //     printf("%s ", invertedIndex[i].key);
    //     for (int j=0;j<invertedIndex[i].indexLen;j++) {
    //         printf("%d ", invertedIndex[i].indexes[j]);
    //     }
    //     printf("\n");
    // }
    // destroy the result after we are done with it
    duckdb_destroy_result(&result);

    // 将索引保存为表
    state = duckdb_query(con, "DROP TABLE IF EXISTS document_content_fts_index;CREATE TABLE document_content_fts_index(words VARCHAR, docId INT);", NULL);
    if (state == DuckDBSuccess) {
        // fprintf(stdout, "创建索引表成功\n");
        duckdb_appender appender;
        if (duckdb_appender_create(con, NULL, "document_content_fts_index", &appender) == DuckDBError) {
        // handle error
            printf("error\n");
        }

        for (int i=0;i<invertedIndexLen;i++) {
            for (int j=0;j<invertedIndex[i].indexLen;j++) {
                // fprintf(stdout, "invertedIndex[i].key: %s\n", invertedIndex[i].key);
                duckdb_append_varchar(appender, invertedIndex[i].key);
                duckdb_append_int32(appender, invertedIndex[i].indexes[j]);
                duckdb_appender_end_row(appender);
            }
        }

        duckdb_appender_destroy(&appender);
    } else {
        fprintf(stderr, "创建索引表失败\n");
    }

    // fprintf(stdout, "索引document_content_fts_index创建成功\n");
}

void deleteItem(duckdb_connection con, char *title) {
    duckdb_state state;
    char deleteStmt[] = "delete from documents where title='";
    char *titleText = malloc(MAX_WORD);
    strcpy(titleText, title+7);
    strcat(deleteStmt, titleText);
    strcat(deleteStmt, "'");
    printf("%s\n", deleteStmt);

    if (duckdb_query(con, deleteStmt, NULL) == DuckDBSuccess) {
        fprintf(stdout, "delete %s success\n", titleText);
    } else {
        fprintf(stderr, "delete error");
    }

    free(titleText);
}

int main() {
    int code;
    char *errMsg[100];

    duckdb_database db;
    duckdb_connection con;

    const char path[100] = "./storage/documents.duckdb";
    
    // char msg[1000];

    if (access(path, F_OK) != 0) {
        createTable(path);
    }

    if (duckdb_open(path, &db) == DuckDBError) {
        // handle error
        printf("open error\n");
        return 1;
    }
    if (duckdb_connect(db, &con) == DuckDBError) {
        // handle error
        printf("connect error\n");
        return 1;
    }

    loadStopWords(con, STOP_WORDS_PATH);
    // exit(0);

    duckdb_prepared_statement stmt;
    if (duckdb_prepare(con, "insert into documents (title, content) values($1, $2)", &stmt) == DuckDBError) {
        // handle error
        printf("prepare error\n");
        return 1;
    }
    Jieba handle = NewJieba(DICT_PATH, HMM_PATH, USER_DICT, IDF_PATH, STOP_WORDS_PATH);
    // 每次开启都刷新一遍索引
    buildIndex(handle, STOP_WORDS_PATH, con, "content");

    while (true) {
        fprintf(stdout,
            "**********************************\n"
            "  --show              输出表中内容\n"
            "  --append            插入文本\n"
            "  --delete title      删除文本\n"
            "  --index             创建索引\n"
            "  --select keyword    搜索查询\n"
            "  --quit              退出程序\n"
            "**********************************\n"
            "--"
        );
    
        char input[300];
        fgets(input, sizeof(input), stdin);
        // printf("get %s\n", input);
        size_t len = strlen(input);
        if (len > 0 && input[len - 1] == '\n') {
            input[len - 1] = '\0';
        }

        if (len > 280) {
            char c;
            while (c=getchar()!='\n' && c!=EOF) {};
        }

        if (strcmp(input, "show") == 0) {
            selectFromDoc(con);
        } else if (strcmp(input, "append") == 0) {
            insertIntoDoc(stmt);
        } else if (strstr(input, "delete ") != NULL) {
            deleteItem(con, strstr(input, "delete "));
        } else if (strcmp(input, "index") == 0) {
            buildIndex(handle, STOP_WORDS_PATH, con, "content");
        } else if (strstr(input, "select ") != NULL) {
            selectByKey(handle, con, strstr(input, "select "));
        } else if (strcmp(input, "quit") == 0) {
            printf("Bye\n");
            duckdb_destroy_prepare(&stmt);
            // cleanup
            duckdb_disconnect(&con);
            duckdb_close(&db);
            exit(0);
        } else {
            printf("选项输入错误，请重新输入。\n");
        }
    }

    return 0;
}