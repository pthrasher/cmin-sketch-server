import zmq
import time

context = zmq.Context()

#  Socket to talk to server
print "Connecting to server..."
socket = context.socket(zmq.REQ)
socket.connect ("tcp://127.0.0.1:9001")
print "connected"

for i in range(100):
    socket.send("hello cruel world")
    print "sent hello cruel world"

    r = socket.recv()
    print "%s received" % r

