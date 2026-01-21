build: client server
	
client:
	gcc -Wall -o build/client src/client.c src/clientSy.c src/error.c

server:
	gcc -Wall -o build/server src/server.c src/serverSy.c src/error.c

clean:
	rm -f build/client build/server

rs:
	./build/server -f output

rc:
	./build/client -a ::1 -f input -w 10

loss:
	./build/server -f output -r 0.3 -a 0.3

ack:
	./build/server -f output -a 0.3

req:
	./build/server -f output -r 0.3


