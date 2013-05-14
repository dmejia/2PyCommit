import threading
from transactions import Transaction

class RecoveryHelper:
	@staticmethod
	# Create transaciton log string out of a transaction obj
	def createTransactionLog(transaction):
		logEntry = None
		if transaction.operationString:
			logEntry = "{0} {1} {2}".format(transaction.tid, transaction.state, transaction.operationString)
		else:
			logEntry = "{0} {1}".format(transaction.tid, transaction.state)
		return logEntry

	@staticmethod
	# Creates a transaction obj out of log entry string
	def parseTransactionLog(logEntry):
		logParts = logEntry.split()
		trAction = None
		
		if len(logParts) >= 3:
			print("Transaction id string: {0}".format(logParts[0]))
			tid = int(logParts[0])
			state = logParts[1]
			operation = logParts[2]
			print("Parsing transaction with tid: {0}, state: {1}, operation: {2}".format(tid, state, operation))
			if state == "master-start-2PC" or state == "master-abort":
				action = lambda replica: replica.abort(tid)
				trAction = Transaction(tid, "master-abort", "", action, "") 
			elif state == "master-commit":
				action = lambda replica: replica.commit(tid)
				trAction = Transaction(tid, state, "", action, "") 
			elif state == "replica-commit":
				if operation == "delete":
					if len(logParts) >= 4:
						key = logParts[3]
						action = lambda store: store.delete(key)	
						trAction = Transaction(tid, state, "delete {0}".format(key), action, key)
				elif operation == "put":
					if len(logParts) >= 5:
						key = logParts[3]
						value = logParts[4]
						action = lambda store: store.put(key, value)
						trAction = Transaction(tid, state, "put {0} {1}".format(key, value), action, key)
			elif state == "replica-yes":
				if operation == "delete":
					if len(logParts) >= 4:
						key = logParts[3]
						action = lambda store: store.delete(key)	
						trAction = Transaction(tid, state, "delete {0}".format(key), action, key)
				elif operation == "put":
					if len(logParts) >= 5:
						key = logParts[3]
						value = logParts[4]
						action = lambda store: store.put(key, value)
						trAction = Transaction(tid, state, "put {0} {1}".format(key, value), action, key)
			else:
				print("Nothing required for state: {0}", state)

		return trAction

	@staticmethod
	def recoverMaster(logFile, master, replicas):
		trActions = RecoveryHelper.parseTransactions(logFile)

		# Execute them
		for tid in trActions:
			trAction = trActions[tid]
			master.transactions[tid] = trAction
			print("Executing action {0} {1} in all replicas".format(trAction.state, trAction.operationString))
			if trAction.action:
				for replica in replicas:
					try:
						threading.Thread(target=trAction.action, args=[replica]).start()
					except Exception as e:
						print("Error sendint recovery action to one of the replicas")
						print("Exception: {0}".format(e))
						pass

		tids = trActions.keys()
		if len(tids) > 0:
			master.idCount = max(tids) + 1
		print("Finished recovery...clearing log")

	@staticmethod
	def recoverReplica(logFile, replica, store):
		trActions = RecoveryHelper.parseTransactions(logFile)

		# Execute them
		for tid in trActions:
			trAction = trActions[tid]
			print("Executing action {0} {1}".format(trAction.state, trAction.operationString))
			if trAction.state == "replica-commit":
				if trAction.action:
					try:
						trAction.action(store)
					except Exception as e:
						print("Error executing aciton")
						print("Exception: {0}".format(e))
						pass
			elif trAction.state == "replica-yes":
				#contact master (block until response)
				#if the transaction is committed, execute action
				#if the state is votereq, add transaction to list 
				#   (and replica will wait for commit/abort)
				#if aborted, don't do anything

				trMasterState = None
				print("Transaction was in uncertainty state, so contacting master")
				while (not trMasterState):
					try:
						trMasterState = replica.masterProxy.transactionState(trAction.tid)
					except Exception as e:
						print("Error contacting master")
						print("Exception: {0}".format(e))
						pass

				if trMasterState == "master-commit":	
					print("Master commited, so executing action")
					if trAction.action:
						try:
							trAction.action(store)
						except Exception as e:
							print("Error executing aciton")
							print("Exception: {0}".format(e))
							pass
				elif trMasterState == "master-start-2pc":
					print("Master hasn't commited yet, so keep the transaciton active (but add timeout)")
					replica._Replica__acquireKeyLock(trAction.key)
					replica.transactions[trAction.tid] = trAction
					replica._Replica__scheduleTerminateProtocol(trAction)
				else:
					print("Master said that transaction was in {0} so not doing anything".format(trMasterState))


		print("Finished recovery...clearing log")

	@staticmethod
	def parseTransactions(logFile):
		trActions = dict()
		while 1:
			line = logFile.readline()
			if not line:
				break
			transaction = RecoveryHelper.parseTransactionLog(line)
			if (transaction):
				trActions[transaction.tid] = transaction
			else:
				print("Log entry not associated to an action: {0}".format(line))
		return trActions

