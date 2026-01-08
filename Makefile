all: client server

client: 
	gcc -Wall -o client src/client.c src/clientSy.c src/error.c

server:
	gcc -Wall -o server src/server.c src/serverSy.c src/error.c

clean:
	rm -f client server

