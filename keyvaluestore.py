import shelve

class KeyValueStore:
	def __init__ (self, dbName):
		print("dbName at the kvs: " + dbName)
		self.data = shelve.open(dbName)

	def put(self, key, value):
		self.data[key] = value
		return True

	def get(self, key):
		return self.data[key] if key in self.data else None

	def delete(self, key):
		success = False
		if key in self.data:
			del self.data[key]
			success = True
		return success

	def close(self):
		self.data.close()
		return True
