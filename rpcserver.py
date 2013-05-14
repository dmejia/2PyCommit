from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer

class MultiThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
	pass
