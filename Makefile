all: client server

client: 
	gcc -Wall -o build/client src/client.c src/clientSy.c src/error.c

server:
	gcc -Wall -o build/server src/server.c src/serverSy.c src/error.c

clean:
	rm -f build/client build/server

rs:
	./build/server

rc:
	./build/client

