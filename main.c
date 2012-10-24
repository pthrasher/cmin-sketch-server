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

char *substring(const char *string, size_t position, size_t length) 
{
    if ((string == 0) || strlen(string) == 0 || strlen(string) < position || strlen(string) < (position+length))
        return 0;

    return strndup(string + position, length);
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

int parseMessage(char * message, char ***words, int *wordsc) {
    int i = 0;
    char const tokenDelim[] = "\r\n";
    int bytes = 0, wordLen = 0;
    char *word;
    *wordsc = 0;

    printf("Parsing Message: '%s'\n", message);

    word = strtok(message, tokenDelim);    // Every msg must start with '*' followed by number of arguments in the msg
    
    if (word != NULL && strlen(word) > 1 && word[0] == '*') {

        // FREEE SUBSTRINGS!!!!!!!!!!!
        char *num_args_str = substring(word, 1, strlen(word) - 1);
        *wordsc = atoi(num_args_str);
        free(num_args_str);
        printf("Arguments: %d\n", *wordsc);
        *words = (char **) calloc(*wordsc, sizeof(char*));    // We have number of words, so we can allocate the array
        if (*words == NULL) {
            printf("ERROR: Out Of Memory\n");
            return -1;
        }
    } else {
        printf("WARNING: Malformed message recieved.\n");
        return -1;
    }

    word = strtok(NULL, tokenDelim);
    while (word != NULL) {
        // first word will be '$' followed by number of bytes to alloc
        wordLen = strlen(word);
        printf("Current Word Length: %d\n", wordLen);
        if (wordLen > 1 && word[0] == '$') {
            char *bytes_str = substring(word, 1, wordLen - 1);
            bytes = atoi(bytes_str) + 1;    // Tacking on an extra character for null term
            free(bytes_str);
            printf("BYTES: %d\n", bytes);
            (*words)[i] = (char *) calloc(bytes, sizeof(char)); 
            if ((*words)[i] == NULL) {
                printf("ERROR: Out Of Memory\n");
                return -1;
            }
        } else {
            printf("WARNING: Malformed message recieved: %s\n", word);
            return -1;
        }

        word = strtok(NULL, tokenDelim);
        printf("'%s' : length %d\n", word, (int)strlen(word));
        if (word != NULL && strlen(word) < bytes) {     // len MUST be LESS than bytes allocated for null term
            printf("Argument: %s\n", word);
            strncpy((*words)[i], word, bytes);
        } else {
            printf("WARNING: Unable to parse argument: %s\n", word);
            return -1;
        }
        printf("COMMAND: %s\n", (*words)[0]);
        word = strtok(NULL, tokenDelim);
        i++;
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

    char **argv;
    //int *argc = (int *)malloc(sizeof(int));
    int argc = 0;

    ret = parseMessage(message, &argv, &argc);
    if (ret == -1) {
        // FREE argv!!!!!!!!!
        int i;
        for (i = 0; i < argc; i++) {
            free(argv[i]);
        }
        free(argv);
        errno = EINVALIDCOMMAND;
        return -1;
    }
    
    // JOHN
    //printf("ARGC = %d\n", *argc);
    printf("ARGC = %d\n", argc);

    printf("ARGV[0] = %s\n", argv[0]);

    printf("About to compare command...\n");

    if (strcasecmp(argv[0], "create") && argc == 4) {
        ret = newField(argv[1], (unsigned int)atoi(argv[2]), (unsigned int)atoi(argv[3]));
        ret = setReply(reply, "OK");
    } else if (strcasecmp(argv[0], "drop") && argc == 2) {
        ret = dropField(argv[1]);
        ret = setReply(reply, "OK");
    } else if (strcasecmp(argv[0], "insert") && argc == 3) {
        printf("Recieved an INSERT\n");
        ret = insertIntoField(argv[1], argv[2]);
        ret = setReply(reply, "OK");
    } else if (strcasecmp(argv[0], "query") && argc == 3) {
        unsigned long result = queryFromField(argv[1], argv[2]);
        const int n = snprintf(NULL, 0, "%lu", result);
        char buf[n+1];
        snprintf(buf, n+1, "%lu", result);
        ret = setReply(reply, buf);
    } else if (strcasecmp(argv[0], "stats") && argc > 1) {
        ret = setReply(reply, "Not implemented yet.");
    } else {
        ret = setReply(reply, "Not implemented yet.");
    }

    // TODO: free argv!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    int i;
    for (i = 0; i < argc; i++) {
        free(argv[i]);
    }
    free(argv);

    return 1;

}


int main(int argc, char const* argv[]) {
    int const REPLY_MSG_SIZE = 11;  // Careful not to send replies larger than 11 chars!
    char const *REPLY_OK_MSG = "OK";
    char const *REPLY_ERROR_MSG = "ERROR";

    int ret;

    errno = 0;

    ret = newServer(&SERVER);
    if (ret != 1) {
        printf("ERROR: Server could not be initialized");
        return shutdown(); /* error */
    }


    for (;;) {
        //  Wait for next request from client
        zmq_msg_t request;
        ret = zmq_msg_init(&request);
        if (ret != 0) {
            printf("ERROR: Request Message could not be initialized");
            return shutdown();
        }

        zmq_msg_t reply;
        ret = zmq_msg_init_size(&reply, REPLY_MSG_SIZE * sizeof(char));
        if (ret != 0) {
            printf("ERROR: Reply Message could not be initialized");
            return shutdown();
        }

        ret = zmq_recv(SERVER.socket, &request, 0);
        if (ret != 0) {
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

        char *message = (char *) zmq_msg_data(&request);

        char msg_status[REPLY_MSG_SIZE];
        ret = handleMessage(&reply, message);
        if (ret == 1) {
            strncpy(msg_status, REPLY_OK_MSG, REPLY_MSG_SIZE);
        } else {
            printf("Error on handleMessage.\n");
            strncpy(msg_status, REPLY_ERROR_MSG, REPLY_MSG_SIZE);
        }
        zmq_msg_close(&request);



        
        memcpy(zmq_msg_data(&reply), (void *)msg_status, REPLY_MSG_SIZE * sizeof(char));



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
