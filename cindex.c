#include <stdio.h>
#include <stdlib.h>

#include "lib/jieba.h"
#include <string.h>

#define MAX_LINES 2000
#define MAX_WORD 1000

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

// 分词并过滤停用词
char **tokenize(char *text, char *lines[], int line_count, int *filteredWordsLen) {

    Jieba handle = NewJieba(DICT_PATH, HMM_PATH, USER_DICT, IDF_PATH, STOP_WORDS_PATH);
    // printf("tokenize\n");
    size_t len = strlen(text);
    CJiebaWord* x;
    CJiebaWord* words = Cut(handle, text, len);

    int flag;
    char **filteredWords = malloc(MAX_WORD * sizeof(char *));
    char buffer[256];

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

            filteredWords[count] = malloc(strlen(buffer) + 1);
            if (filteredWords[count] == NULL) {
                perror("内存分配失败");
                return NULL;
            }

            strncpy(filteredWords[count], x->word, x->len);
            
            count++;
        }
    }

    *filteredWordsLen = count;

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
            invertedIndex[n].key = malloc(sizeof(filteredWords[n]));

            strcpy(invertedIndex[n].key, filteredWords[i]);

            invertedIndex[n].indexes = malloc(sizeof(int));
            invertedIndex[n].indexes[0] = docId;
            invertedIndex[n].indexLen = 1;

            n += 1;
        }
    }

    *invertedIndexLen = n;
}

int main() {
    Jieba handle = NewJieba(DICT_PATH, HMM_PATH, USER_DICT, IDF_PATH, STOP_WORDS_PATH);

    char *documents[1000] = {
        "我喜欢跑步和游泳。",
        "跑步是一项很好的运动。",
        "游泳可以锻炼全身肌肉。",
        "我喜欢跑步和游泳，因为它们很有趣。"
    };

    // inverted_index(documents, 4);
    // 加载停用词
    printf("加载停用词\n");
    FILE *fin;
    fin = fopen(STOP_WORDS_PATH, "r");

    if(fin == NULL) {
        printf("停用词文件打开失败\n");
        return 1;
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
            return EXIT_FAILURE;
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
    char **filteredWords;
    int filteredWordsLen;
    indexMap *invertedIndex = malloc(sizeof(indexMap)*MAX_WORD);
    int invertedIndexLen = 0;

    for (int i=0;i<4;i++) {
        filteredWords = tokenize(documents[i], lines, line_count, &filteredWordsLen);

        inverted_index(invertedIndex, &invertedIndexLen, filteredWords, filteredWordsLen, i);
    }

    for (int i=0;i<invertedIndexLen;i++) {
        printf("%s ", invertedIndex[i].key);
        for (int j=0;j<invertedIndex[i].indexLen;j++) {
            printf("%d ", invertedIndex[i].indexes[j]);
        }
        printf("\n");
    }
    
    char query_text[] = "跑步 游泳";
    char **queryWords;
    int queryWordsLen = 0;
    queryWords = tokenize(query_text, lines, line_count, &queryWordsLen);

    printf("\n查询文本的分词\n");
    for (int i=0;i<queryWordsLen;i++) {
        printf("%s ", queryWords[i]);
    }
    printf("\n");

    int *res = malloc(sizeof(int)*MAX_WORD);
    int *resTmp = malloc(sizeof(int)*MAX_WORD);
    indexCount *indexCounts = malloc(sizeof(indexCount)*1000);
    int capacity = MAX_WORD;
    int resCount = 0;
    int resTmpCount = 0;
    indexCount *head = malloc(sizeof(indexCount));
    indexCount *p;

    for (int i=0;i<queryWordsLen;i++) {
        for (int j=0;j<invertedIndexLen;j++) {
            if (strcmp(invertedIndex[j].key, queryWords[i]) == 0) {
                if (resCount == 0) {
                    indexCount *tmpIndexCount = malloc(sizeof(indexCount));
                    tmpIndexCount->index = invertedIndex[j].indexes[0];
                    tmpIndexCount->count = 1;
                    tmpIndexCount->next = NULL;
                    head->next = tmpIndexCount;

                    for (int k=1;k<invertedIndex[j].indexLen;k++) {
                        indexCount *tmpIndexCount = malloc(sizeof(indexCount));
                        tmpIndexCount->index = invertedIndex[j].indexes[k];
                        tmpIndexCount->count = 1;
                        tmpIndexCount->next = head->next;

                        head->next = tmpIndexCount;
                    }
                } else {
                    for (int k=0;k<invertedIndex[j].indexLen;k++) {
                        p = head->next;
                        while (p) {
                            if (p->index == invertedIndex[j].indexes[k]) {
                                p->count += 1;
                                break;
                            }
                            p = p->next;
                        }
                        if (p==NULL) {
                            indexCount *tmpIndexCount = malloc(sizeof(indexCount));
                            tmpIndexCount->index = invertedIndex[j].indexes[k];
                            tmpIndexCount->count = 1;
                            tmpIndexCount->next = head->next;

                            head->next = tmpIndexCount;
                        }
                    }

                }
                resCount++;

                
                break;
            }
        }
    }

    p = head->next;
    int i = 0;
    while (p) {
        if (p->count == resCount) {
            res[i++] = p->index;
        }
        p = p->next;
    }

    for (int j=0;j<i;j++) {
        printf("%d ", res[j]);
    }
    printf("\n");

    return 0;
}