all: cindex
cindex: libjieba.a
	gcc -o cindex cindex.c -L./ -ljieba -lstdc++ -lm
libjieba.a:
	g++ -o jieba.o -c -DLOGGING_LEVEL=LL_WARNING -I./deps/ lib/jieba.cpp
	ar rs libjieba.a jieba.o 
clean:
	rm -f *.a *.o cindex
