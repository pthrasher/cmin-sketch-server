import zmq
import time

context = zmq.Context()

#  Socket to talk to server
print "Connecting to server..."
socket = context.socket(zmq.REQ)
socket.connect ("tcp://127.0.0.1:9001")
print "connected"

def serialize(*parts):
    message = "*%d\r\n" % len(parts)

    for part in parts:
        message += "$%d\r\n%s\r\n" % (len(part), part)

    return message

def sendmsg(message):
    socket.send(serialize(message.split(' ')))
    print "sent %s" % message
    r = socket.recv()
    print "%s received" % r

for i in range(100):
    msg = "create myfield 30 30"
    sendmsg(msg)
    msg = "insert myfield birds"
    sendmsg(msg)
    msg = "insert myfield birds"
    sendmsg(msg)
    msg = "insert myfield birds"
    sendmsg(msg)
    msg = "insert myfield halp"
    sendmsg(msg)
    msg = "insert myfield boom"
    sendmsg(msg)
    msg = "insert myfield boom"
    sendmsg(msg)
    msg = "insert myfield boom"
    sendmsg(msg)
    msg = "insert myfield boom"
    sendmsg(msg)
    msg = "insert myfield boom"
    sendmsg(msg)
    msg = "insert myfield zoom"
    sendmsg(msg)
    msg = "insert myfield zoom"
    sendmsg(msg)
    msg = "insert myfield zoom"
    sendmsg(msg)
    
    msg = "query myfield birds"
    sendmsg(msg)
    msg = "query myfield halp"
    sendmsg(msg)
    msg = "query myfield boom"
    sendmsg(msg)
    msg = "query myfield zoom"
    sendmsg(msg)
