import random
import socket
import threading
import transactions
import recovery

class Master:
	def __init__(self, logFileName, replicaProxies):
		self.replicaProxies = replicaProxies
		self.idCount = 0
		self.logFileName = logFileName
		self.transactions = dict()
		self.__recover()
		self.logFile = open(self.logFileName, "w")
		self.tidLock = threading.Lock()

	def get(self, key):
		return self.__get(key)

	def put(self, key, value):
		return self.__2phaseCommit(lambda replica, tid: replica.put(key,value,tid), "put {0} {1}".format(key, value), key)

	def delete(self, key):
		return self.__2phaseCommit(lambda replica, tid: replica.delete(key, tid), "delete {0}".format(key), key)

	def transactionState(self, tid):
		if tid in self.transactions:
			tr = self.transactions[tid]
			return tr.state
		return "unknown"

	def __2phaseCommit(self, func, funcName, key):
		transaction = self.__createTransaction(func, funcName, key)
		self.__executeOperation(transaction)	
		allYes = self.__requestVotes(transaction)
		if allYes:
			self.__commit(transaction)
		else:
			self.__abort(transaction)

		return allYes

	def __createTransaction(self, func, funcName, key):
		tid = -1
		with self.tidLock:
			tid = self.idCount
			self.idCount += 1
		transaction = transactions.Transaction(tid, "master-start", funcName, func, key)
		self.transactions[transaction.tid] = transaction
		print ("Started transaction {0}".format(tid))
		return transaction

	def __executeOperation(self, transaction):
		success = False
		print("Start sending {0} operation".format(transaction.operationString))
		for replica in self.replicaProxies:
			try:
				transaction.action(replica, transaction.tid)
				success = True
			except Exception as e:
				print("Error sending operation to one of the replicas")
				print("Exception: {0}".format(e))
				pass
		return success

	def __requestVotes(self, transaction):
		transaction.state = "master-start-2pc"
		self.__log(transaction)

		print("Sending votereqs")
		allYes = True
		try:
			for replica in self.replicaProxies:
				allYes = allYes and replica.voteReq(transaction.tid)
		except Exception as e:
			print("Error sending voteReq to one of the replicas")
			print("Exception: {0}".format(e))
			allYes = False

		return allYes

	def __commit(self, transaction):
		transaction.state = "master-commit"
		self.__log(transaction)
		for replica in self.replicaProxies:
			try:
				print("Sending {0} to replica".format(transaction.state))
				replica.commit(transaction.tid)
				print("Sent")
			except Exception as e:
				print("Error sending final commit decision to one of the replicas")
				print("Exception: {0}".format(e))
				pass


	def __abort(self, transaction):
		transaction.state = "master-abort"
		self.__log(transaction)
		for replica in self.replicaProxies:
			try:
				print("Sending {0} to replica".format(transaction.state))
				replica.abort(transaction.tid)
				print("Sent")
			except Exception as e:
				print("Error sending final abort decision to one of the replicas")
				print("Exception: {0}".format(e))
				pass

	def __get(self, key):
		replicas = self.replicaProxies[:]
		done = False
		while (not done and len(replicas) > 0):
			ix = random.randint(0, len(replicas)-1)
			replica = replicas[ix]
			try:
				value = replica.get(key)
				done = True
			except Exception as e:
				print("Error getting value from replica")
				print("Exception {0}".format(e))
				del replicas[ix]


		if not done:
			raise EnvironmentError("System is temporarily unavailable")

		return value


	def __log(self, transaction):
		logEntry = recovery.RecoveryHelper.createTransactionLog(transaction)
		self.logFile.write(logEntry + "\n")
		self.logFile.flush()

	def __recover(self):
		print("Starting recovery")
		try:
			logFile = open(self.logFileName, "r")
			recovery.RecoveryHelper.recoverMaster(logFile, self, self.replicaProxies)
			logFile.close()
		except IOError as e:
			print("Error opening file: {0}".format(e))
