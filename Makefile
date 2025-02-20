all: test
test: libjieba.a
	gcc -o test test.c -L./ -L/home/luwei/code/memAss/lib -ljieba -lstdc++ -lm -lduckdb
libjieba.a:
	g++ -o jieba.o -c -DLOGGING_LEVEL=LL_WARNING -I./deps/ lib/jieba.cpp
	ar rs libjieba.a jieba.o 
clean:
	rm -f *.a *.o test
