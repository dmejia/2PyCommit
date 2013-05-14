import xmlrpc.client
import master
import mastermock
import sys
from rpcserver import MultiThreadXMLRPCServer

def createProxy(port):
	proxy = xmlrpc.client.ServerProxy("http://localhost:"+ port)
	return proxy

if len(sys.argv) < 2:
	print ("startmaster logFileName replica-port1 [replica-port2 ...]")
	exit()

logFileName = sys.argv[1]
server = MultiThreadXMLRPCServer(("localhost", 8000), allow_none=True)
print("Listening on port 8000...")

replicaProxies = [createProxy(arg) for arg in sys.argv[2:len(sys.argv)]]

server.register_instance(mastermock.MasterMock(logFileName, replicaProxies))
server.serve_forever()

