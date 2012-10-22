#include <stdio.h>
#include <stdlib.h>
#include <zmq.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <avl.h>

typedef struct _field {
    unsigned int width;
    unsigned int depth;
    unsigned long ** rows;
} field_t;

typedef struct _server {
    void * context;
    void * socket;
    long requestCount;
    long replyCount;
    field_t * field;
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

int newField(field_t * field, unsigned int width, unsigned int depth) {

    field->width = width;
    field->depth = depth;

    unsigned long ** rows = (unsigned long**)malloc(depth * sizeof(unsigned long*));
    if (NULL == rows) {
        return -1; /* error */
    }

    unsigned int a;
    unsigned int b;
    unsigned long * row;
    for (a = 0; a < depth; a++) {
        row = (unsigned long*)malloc(width * sizeof(unsigned long));
        for (b = 0; b < width; b++) {
            row[b] = 0;
        }
        rows[a] = row;
    }

    field->rows = rows;
    return 1;
}

int createField(char * name, unsigned int width, 
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

char * zmsgToString(zmq_msg_t * message) {
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
int handleMessage(zmq_msg_t * reply, char * message) {
    int ret;



}


int main(int argc, char const* argv[]) {

    int ret;

    errno = 0;
    field_t field;

    ret = newServer(&SERVER);
    if (ret == -1) {
        return shutdown(); /* error */
    }

    ret = newField(&field, 10, 10);
    if (ret == -1) {
        return shutdown(); /* error */
    }

    SERVER.field = &field;
    errno = 0;

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
