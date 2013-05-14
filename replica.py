import keyvaluestore 
import threading 
import transactions
import recovery

class Replica:

	TIMEOUT = 10

	def __init__(self, logFileName, dbName, port, masterProxy):
		self.store = keyvaluestore.KeyValueStore(dbName)
		self.port = port
		self.masterProxy = masterProxy
		self.keyLocksDict = dict()
		self.keyLocksDictLock = threading.Lock()
		self.transactionLocksDict = dict()
		self.transactionLocksDictLock = threading.Lock()

		self.transactions = dict()
		self.logFileName = logFileName
		self.__recover()
		self.logFile = open(self.logFileName, "w")
	
	def put(self, key, value, tid):
		success = False
		if self.__acquireKeyLock(key):
			action = lambda s: s.put(key, value)	
			transaction = transactions.Transaction(tid, "operate", "put {0} {1}".format(key, value), action, key)
			self.transactions[tid] = transaction
			timer = threading.Timer(Replica.TIMEOUT, self.__tryAbort, args=[transaction])
			timer.start()
			success = True
		else:
			print("Lock not acquired for put {0} {1}".format(key, value))

		return success

	def get(self, key):
		# Note that one could also lock gets while there are transactions active on the key, but this way
		# gets will continue to be served in parallel and will eventually be consistent
		return self.store.get(key)	

	def delete(self, key, tid):
		success = False
		if self.__acquireKeyLock(key):
			action = lambda s: s.delete(key)	
			transaction = transactions.Transaction(tid, "operate", "delete {0}".format(key), action, key)
			self.transactions[tid] = transaction
			timer = threading.Timer(Replica.TIMEOUT, self.__tryAbort, args=[transaction])
			timer.start()
			success = True
		else:
			print("Lock not acquired for delete {0}".format(key))

		return success

	def voteReq(self, tid):
		success = False
		self.__acquireTransactionLock(tid)
		if tid in self.transactions:
			print("Transaction found, voting Yes")
			transaction = self.transactions[tid]
			transaction.state = "replica-yes"
			self.__log(transaction)

			# after voting yes, the replica cannot take a decision anymore, and so after a timeout
			# it needs to run a termination protocol (ask the master) to find about the outcome
			self.__scheduleTerminateProtocol(transaction)
			success = True
		else:
			print("Transaction not found, voting No")
			transaction = transactions.Transaction(tid, "replica-no", None, None, None)
			self.__log(transaction)

		self.__releaseTransactionLock(tid)
		return success
		
	def commit(self, tid):
		success = False
		# Note that the transaction lock is not needed (neither is a check for state==replica-yes
		# because the master can never commit unless the replica already voted yes
		if tid in self.transactions:
			print("Transaction found, executing")
			transaction = self.transactions[tid]
			transaction.state = "replica-commit"
			self.__log(transaction)
			transaction.action(self.store)
			self.__releaseKeyLock(transaction.key)
			del self.transactions[tid]
			success = True
			print("Transaction successful!") 
		else:
			print("Transaction not found, likely executed already")
		return success

	def abort(self, tid):
		self.__acquireTransactionLock(tid)
		if tid in self.transactions:
			print("Transaction found, aborting")
			transaction = self.transactions[tid]
			transaction.state = "replica-abort"
			self.__log(transaction)
			self.__releaseKeyLock(transaction.key)
			del self.transactions[tid]
		else:
			print("Transaction not found, likely executed already")
		self.__releaseTransactionLock(tid)
		return True

	def __recover(self):
		print("Starting recovery")
		try:
			logFile = open(self.logFileName, "r")
			recovery.RecoveryHelper.recoverReplica(logFile, self, self.store)
			logFile.close()
		except IOError as e:
			print("Error opening file: {0}".format(e))

	def __log(self, transaction):
		logEntry = recovery.RecoveryHelper.createTransactionLog(transaction)
		self.logFile.write(logEntry + "\n")
		self.logFile.flush()

	def __acquireKeyLock(self, key):
		# Note that this there won't be a deadlock because the inner lock will never block
		with self.keyLocksDictLock:
			if key in self.keyLocksDict:
				return self.keyLocksDict[key].acquire(blocking=False)
			else:
				lock = threading.Lock()
				lock.acquire()
				self.keyLocksDict[key] = lock
				return True

	def __releaseKeyLock(self, key):
		with self.keyLocksDictLock:
			self.keyLocksDict[key].release()
		
	def __acquireTransactionLock(self, tid):
		lock = None
		with self.transactionLocksDictLock:
			if tid in self.transactionLocksDict:
				lock = self.transactionLocksDict[tid]
			else:
				lock = threading.Lock()
				self.transactionLocksDict[tid] = lock
		lock.acquire()

	def __releaseTransactionLock(self, tid):
		with self.transactionLocksDictLock:
			self.transactionLocksDict[tid].release()

	def __scheduleTerminateProtocol(self, transaction):
		timer = threading.Timer(Replica.TIMEOUT, self.__terminateProtocol, args=[transaction])
		timer.start()

	def __tryAbort(self, transaction):
		# if transaction is still blocked waiting for the vote request, abort
		if transaction.state == "operate":
			print("Timed out waiting for votereq, so abort")
			self.abort(transaction.tid)

	def __terminateProtocol(self, transaction):
		trMasterState = None
		print("Transaction was in uncertainty state, so contacting master")
		while (not trMasterState and transaction.state == "replica-yes"):
			try:
				trMasterState = self.masterProxy.transactionState(transaction.tid)
			except Exception as e:
				print("Error contacting master")
				print("Exception: {0}".format(e))
				pass

		# if got state from polling replica, execute termination, otherwise the commit/abort was received
		if trMasterState:
			if trMasterState == "master-commit":	
				print("Master commited, so commit")
				self.commit(transaction.tid)
			elif trMasterState == "master-start-2pc":
				print("Master taking its time, so keep waiting")
				timer = threading.Timer(Replica.TIMEOUT, self.__terminateProtocol, args=[transaction])
				timer.start()
			else:
				print("Master said {0} so abort".format(trMasterState))
				self.abort(transaction.tid)
