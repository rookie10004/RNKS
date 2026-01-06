build:
	gcc -Wall -o client.o src/client.c
	gcc -Wall -o server.o src/server.c
clean:
	rm client.o
	rm server.o

run:
	./client.o
	./server.o