build:
	gcc -Wall -o app.o src/main.c src/client.c src/server.c src/utils.c

clean:
	rm app.o

run:
	./app.o