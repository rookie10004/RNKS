#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

int main()
{
    int socket_fd; //socket file descriptor

    socket_fd = socket(AF_INET6, SOCK_DGRAM, 0); //IPv6, UDP(datagramme)

    if(socket_fd < 0)
    {
        perror("socket creation error!");
        return -1;
    }

    printf("client socket: %d\n", socket_fd);

    close(socket_fd);
    
    return 0;
}