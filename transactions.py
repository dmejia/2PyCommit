class Transaction:
	def __init__(self, tid, state, operationString, action, key):
		# (string) transaction id
		self.tid = tid
		# (string) Commit, abort, etc
		self.state = state
		# (string) e.g put key value, delete key, etc
		self.operationString = operationString
		# lambda to execute the actual operation
		self.action = action
		# key, used for locking purposes
		self.key = key

