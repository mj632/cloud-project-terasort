clean:
	rm -rf *.o mySortBenchmark2 mySortBenchmark20

all:clean
	gcc -Wall mySortBenchmark_2GB.c -o mySortBenchmark2 -lpthread
	gcc -Wall mySortBenchmark_20GB.c -o mySortBenchmark20 -lpthread
