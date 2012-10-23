#include <stdio.h>
#include <stdlib.h>
#include <zmq.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include "avl.h"
#include "sketches.h"
#include "murmur.h"


#define EINVALIDCOMMAND 201

typedef struct _server {
    void * context;
    void * socket;
    long requestCount;
    long replyCount;
} server_t;

/*
EINVAL
The endpoint supplied is invalid.
EPROTONOSUPPORT
The requested transport protocol is not supported.
ENOCOMPATPROTO
The requested transport protocol is not compatible with the socket type.
EADDRINUSE
The requested address is already in use.
EADDRNOTAVAIL
The requested address was not local.
ENODEV
The requested address specifies a nonexistent interface.
ETERM
The ØMQ context associated with the specified socket was terminated.
ENOTSOCK
The provided socket was invalid.
EMTHREAD
No I/O thread is available to accomplish the task.
No I/O thread is available to accomplish the task.
 */
int newServer(server_t * server) {
    server->context = zmq_init(1);
    server->socket = zmq_socket(server->context, ZMQ_REP);
    server->requestCount = 0;
    server->replyCount = 0;

    int ret = zmq_bind(server->socket, "tcp://*:9001");
    if (ret == -1) {
        if (errno == EINVAL) {
            printf("EINVAL on bind.\n");
            return -1;
        } else if (errno == EPROTONOSUPPORT) {
            printf("EPROTONOSUPPORT on bind.\n");
            return -1;
        } else if (errno == ENOCOMPATPROTO) {
            printf("ENOCOMPATPROTO on bind.\n");
            return -1;
        } else if (errno == EADDRINUSE) {
            printf("EADDRINUSE on bind.\n");
            return -1;
        } else if (errno == EADDRNOTAVAIL) {
            printf("EADDRNOTAVAIL on bind.\n");
            return -1;
        } else if (errno == ENODEV) {
            printf("ENODEV on bind.\n");
            return -1;
        } else if (errno == ETERM) {
            printf("ETERM on bind.\n");
            return -1;
        } else if (errno == ENOTSOCK) {
            printf("ENOTSOCK on bind.\n");
            return -1;
        } else if (errno == EMTHREAD) {
            printf("EMTHREAD on bind.\n");
            return -1;
        } else {
            printf("Unknown error on bind.\n");
            return -1;
        }
    }
    return 1;
}

server_t SERVER;


/*
EAGAIN
Non-blocking mode was requested and no messages are available at the moment.
ENOTSUP
The zmq_recv() operation is not supported by this socket type.
EFSM
The zmq_recv() operation cannot be performed on this socket at the moment due to the socket not being in the appropriate state. This error may occur with socket types that switch between several states, such as ZMQ_REP. See the messaging patterns section of zmq_socket(3) for more information.
ETERM
The ØMQ context associated with the specified socket was terminated.
ENOTSOCK
The provided socket was invalid.
EINTR
The operation was interrupted by delivery of a signal before a message was available.
EFAULT
The message passed to the function was invalid.
 */

int shutdown() {
    printf("Shutdown...\n");
    zmq_close(SERVER.socket);
    zmq_term(SERVER.context);
    return 1;
}

char *substring(char *string, int position, int length) 
{
   char *pointer;
   int c;
 
   pointer = malloc(length+1);
 
   if (pointer == NULL)
   {
      printf("Unable to allocate memory.\n");
      exit(EXIT_FAILURE);
   }
 
   for (c = 0 ; c < position -1 ; c++) 
      string++; 
 
   for (c = 0 ; c < length ; c++)
   {
      *(pointer+c) = *string;      
      string++;   
   }
 
   *(pointer+c) = '\0';
 
   return pointer;
}


/* Language spec:

    CREATE <sketch-name> <width> <depth>;
    DROP <sketch-name>;
    INSERT <sketch-name> <value>;
        Returns OK.
    QUERY <sketch-name> <value>;
        Returns count for the value queried.
    STATS;
        Returns stats about the server.

 */

int parseMessage(char * message, char ** words, int wordsc) {

    int numArgs = 0;
    int wordLen = 0;
    char * word;
    char * tmp;
    int needsArgCount = 1;
    int needsWordLen = 1;
    int i = 0;


    word = strtok(message, "\r\n");
    while (word != NULL) {
        if (wordLen == 0) {
            wordLen = strlen(word);
        }

        if (word[0] == '*' && needsArgCount) {
            tmp = substring(word, 1, wordLen - 1);
            numArgs = atoi(tmp);
            needsArgCount = 0;
            wordLen = 0;
        } else if (word[0] == '$' && needsWordLen) {
            tmp = substring(word, 1, wordLen - 1);
            wordLen = atoi(tmp);
            needsWordLen = 0;
        } else {
            words[i] = word;
            needsWordLen = 1;
            wordLen = 0;
            i++;
        }

        word = strtok(message, "\r\n");
    }

    return 1;
}

int setReply (zmq_msg_t * reply, char * message) {
    size_t len = strlen(message);
    memcpy(zmq_msg_data(reply), message, len);
    return 1;
}

int handleMessage (zmq_msg_t * reply, char * message) {
    int ret;

    char ** argv;
    int argc;

    ret = parseMessage(message, argv, argc);
    if (ret == -1) {
        errno = EINVALIDCOMMAND;
        return -1;
    }

    if (strcasecmp(argv[0], "create")) {
        ret = newField(argv[1], (unsigned int)atoi(argv[2]), (unsigned int)atoi(argv[3]));
        ret = setReply(reply, "OK");
    } else if (strcasecmp(argv[0], "drop")) {
        ret = dropField(argv[1]);
        ret = setReply(reply, "OK");
    } else if (strcasecmp(argv[0], "insert")) {
        ret = insertIntoField(argv[1], argv[2]);
        ret = setReply(reply, "OK");
    } else if (strcasecmp(argv[0], "query")) {
        unsigned long result = queryFromField(argv[1], argv[2]);
        const int n = snprintf(NULL, 0, "%lu", result);
        char buf[n+1];
        int c = snprintf(buf, n+1, "%lu", result);
        ret = setReply(reply, buf);
    } else if (strcasecmp(argv[0], "stats")) {
        ret = setReply(reply, "Not implemented yet.");
    } else {
        ret = setReply(reply, "Not implemented yet.");
    }
    return 1;

}


int main(int argc, char const* argv[]) {

    int ret;

    errno = 0;

    ret = newServer(&SERVER);
    if (ret == -1) {
        return shutdown(); /* error */
    }


    for (;;) {
        //  Wait for next request from client
        zmq_msg_t request;
        zmq_msg_init(&request);

        zmq_msg_t reply;
        zmq_msg_init(&reply);

        int size = 0;
        char * message;

        ret = zmq_recv(SERVER.socket, &request, 0);
        if (ret == -1) {
            if (errno == EINTR) {
                printf("EINTR on RECV.\n");
                return shutdown();
            } else if (errno == EAGAIN) {
                printf("EAGAIN on RECV.\n");
                return shutdown();
            } else if (errno == ENOTSUP) {
                printf("ENOTSUP on RECV.\n");
                return shutdown();
            } else if (errno == EFSM) {
                printf("EFSM on RECV.\n");
                return shutdown();
            } else if (errno == ETERM) {
                printf("ETERM on RECV.\n");
                return shutdown();
            } else if (errno == ENOTSOCK) {
                printf("ENOTSOCK on RECV.\n");
                return shutdown();
            } else if (errno == EFAULT) {
                printf("EFAULT on RECV.\n");
                return shutdown();
            } else {
                printf("Unknown error on RECV.\n");
                return shutdown();
            }
        }

        printf ("Received message\n");
        size = ret;
        message = malloc(size + 1);
        memcpy(message, zmq_msg_data(&request), size);
        zmq_msg_close(&request);
        message[size] = 0;

        ret = handleMessage(&reply, message);
        if (ret == -1) {
            printf("Error on handleMessage.\n");
            return shutdown();
        }


        ret = zmq_send(SERVER.socket, &reply, 0);
        if (ret == -1) {
            if (errno == EINTR) {
                printf("EINTR on SEND.\n");
                return shutdown();
            } else if (errno == EAGAIN) {
                printf("EAGAIN on SEND.\n");
                return shutdown();
            } else if (errno == ENOTSUP) {
                printf("ENOTSUP on SEND.\n");
                return shutdown();
            } else if (errno == EFSM) {
                printf("EFSM on SEND.\n");
                return shutdown();
            } else if (errno == ETERM) {
                printf("ETERM on SEND.\n");
                return shutdown();
            } else if (errno == ENOTSOCK) {
                printf("ENOTSOCK on SEND.\n");
                return shutdown();
            } else if (errno == EFAULT) {
                printf("EFAULT on SEND.\n");
                return shutdown();
            } else {
                printf("Unknown error on SEND.\n");
                return shutdown();
            }

        } else {
            printf("Send call success.\n");
        }
        zmq_msg_close(&reply);
    }
    //  We never get here but if we did, this would be how we end
    zmq_close (SERVER.socket);
    zmq_term (SERVER.context);
    return 0;
}
