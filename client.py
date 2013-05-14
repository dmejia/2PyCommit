import xmlrpc.client

proxy = xmlrpc.client.ServerProxy("http://localhost:8000")

print("Blah has: %s" % proxy.get("blah"))
print("Bluh has: %s" % proxy.get("bluh"))
print("Adding yea to key")
success = proxy.put("key", "yea")
#success = True
if (success):
	print("key has: %s" % proxy.get("key"))
	print("Deleting key")
	success = proxy.delete("key")
	if (success):
		print("Blah has: %s" % proxy.get("key"))
	else:
		print("Del was not successful")

else:
	print("Put was not successful")

