from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

# Extends the XML RPC server to make it multi-threaded
class MultiThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
	pass
