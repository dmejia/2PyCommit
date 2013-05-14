import master

class MasterMock(master.Master):
	def getBlah(self):
		return self.get("blah")

	def putBlah(self, value):
		print("putting blah with {0}".format(value))
		return self._Master__2phaseCommit(lambda replica, tid: replica.put("blah",value,tid), "put blah {0}".format(value))

	def startPut(self, key, value):
		transaction = self._Master__createTransaction(lambda replica, tid: replica.put(key,value,tid), "put {0} {1}".format(key, value), key)
		return transaction.tid

	def startDelete(self, key):
		transaction = self._Master__createTransaction(lambda replica, tid: replica.delete(key,tid), "delete {0}".format(key))
		return transaction.tid

	def execute(self, tid):
		transaction = self.transactions[tid]
		self._Master__executeOperation(transaction)	

	def requestVotes(self, tid):
		transaction = self.transactions[tid]
		return self._Master__requestVotes(transaction)

	def logCommit(self, tid):
		transaction = self.transactions[tid]
		transaction.state = "master-commit"
		self._Master__log(transaction)
		return True

	def logAbort(self, tid):
		transaction = self.transactions[tid]
		transaction.state = "master-abort"
		self._Master__log(transaction)
		return True

	def commit(self, tid):
		transaction = self.transactions[tid]
		return self._Master__commit(transaction)

	def abort(self, tid):
		transaction = self.transactions[tid]
		return self._Master__abort(transaction)




