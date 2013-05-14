import xmlrpc.client
import sys
import replica
from rpcserver import MultiThreadXMLRPCServer

if len(sys.argv) < 3:
	print("startreplica logFileName replica-port [db name]")
	exit()

server = MultiThreadXMLRPCServer(("localhost", int(sys.argv[2])), allow_none=True)
port = sys.argv[2]
print("Listening on port" + port + "...")
masterProxy = xmlrpc.client.ServerProxy("http://localhost:8000")

logFileName = sys.argv[1]
dbName = "someDb{0}".format(port) if len(sys.argv) == 3 else sys.argv[3]
print(dbName)

server.register_instance(replica.Replica(logFileName, dbName, port, masterProxy))
server.serve_forever()
