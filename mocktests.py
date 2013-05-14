import unittest
import subprocess
import xmlrpc.client
import os
import time
import testingbase
from replica import Replica

class MockTests(testingbase.TestingBase):
	def setUp(self):
		self._initialize()

	def test_WhileTransactionNotFinished_KeyIsBlocked(self):
		key = "somekey"
		value = "somevalue"
		value2 = "someothervalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		success = self.masterProxy.put(key, value2)
		self.assertFalse(success)

		self.masterProxy.requestVotes(tid)
		success = self.masterProxy.put(key, value2)
		self.assertFalse(success)

		self.masterProxy.commit(tid)
		success = self.masterProxy.put(key, value2)
		self.assertTrue(success)

		self.assertEqual(value2, self.masterProxy.get(key))

	def test_WhileTransactionNotFinished_KeyIsBlocked(self):
		key = "somekey"
		value = "somevalue"
		value2 = "someothervalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		success = self.masterProxy.put(key, value2)
		self.assertFalse(success)

		self.masterProxy.requestVotes(tid)
		success = self.masterProxy.put(key, value2)
		self.assertFalse(success)

		self.masterProxy.commit(tid)
		success = self.masterProxy.put(key, value2)
		self.assertTrue(success)

		self.assertEqual(value2, self.masterProxy.get(key))

	def test_WhenReplicasDieAfterVotingAndMasterHasntDecided_KeyGetsBlockedOnRecoveryUntilDecisionIsReceived(self):
		key = "somekey"
		value = "somevalue"
		value2 = "someothervalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()
		self._startReplica1()
		self._startReplica2()

		success = self.masterProxy.put(key, value2)
		self.assertFalse(success)

		self.masterProxy.commit(tid)
		success = self.masterProxy.put(key, value2)
		self.assertTrue(success)

		self.assertEqual(value2, self.masterProxy.get(key))

	def test_WhenReplicasDieBeforeVoting_TheyDecideAbortOnRecovery(self):
		key = "somekey"
		value = "somevalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)

		self._killReplica1()
		self._killReplica2()
		self._startReplica1()
		self._startReplica2()

		allYes = self.masterProxy.requestVotes(tid)
		self.assertFalse(allYes)

		self.assertEqual(None, self.masterProxy.get(key))

	def test_WhenAReplicaHasntReceivedCommitMessage_ItDoesntServeGetRequestsOnThatKey(self):
		key = "somekey"
		value = "somevalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		#Fake commit that didn't get to replica2
		self.masterProxy.logCommit(tid)
		self.replica1Proxy.commit(tid)

		#Kill only replica that knows the value was commited
		self._killReplica1()

		#The only replica is uncertain on the value of that key
		with self.assertRaises(Exception):
			self.masterProxy.get(key)

	
		self._startReplica1()	
		self.assertEqual(value, self.masterProxy.get(key))


	def test_WhenReplicasTimeoutWaitingForVoteRequest_TheyDecideAbort(self):
		key = "somekey"
		value = "somevalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)

		time.sleep(Replica.TIMEOUT)
		

		allYes = self.masterProxy.requestVotes(tid)
		self.assertFalse(allYes)

		self.assertEqual(None, self.masterProxy.get(key))

	def test_WhenReplicasTimeoutWaitingForFinalDecision_TheyContactMasterForCommitDecision(self):
		key = "somekey"
		value = "somevalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)
		self.masterProxy.logCommit(tid)

		time.sleep(Replica.TIMEOUT)
		
		self.assertEqual(value, self.masterProxy.get(key))

	def test_WhenReplicasTimeoutWaitingForFinalDecision_TheyContactMasterForAbortDecision(self):
		key = "somekey"
		value = "somevalue"

		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)
		self.masterProxy.logAbort(tid)

		time.sleep(Replica.TIMEOUT)

		self.assertEqual(None, self.masterProxy.get(key))

	def test_WhenMasterDiesBeforeCommit_SendsAbortOnRecovery(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)
		
		self._killMaster()
		self._startMaster()

		self.assertEqual(None, self.masterProxy.get(key))

	def test_WhenMasterDiesAfterLoggingCommitButBeforeSendingIt_SendsCommitOnRecovery(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)
		self.masterProxy.logCommit(tid)
		
		self._killMaster()
		self._startMaster()

		self.assertEqual(value, self.masterProxy.get(key))

	def test_WhenMasterDiesAfterLoggingAbortButBeforeSendingIt_SendsAbortOnRecovery(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)
		self.masterProxy.logAbort(tid)
		
		self._killMaster()
		self._startMaster()

		self.assertEqual(None, self.masterProxy.get(key))

	def test_WhenOneReplicaIsDownDuringVoting_MasterChoosesAbort(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)

		self._killReplica1()

		decideCommit = self.masterProxy.requestVotes(tid)

		self.assertFalse(decideCommit)


	def test_WhenReplicasDieAfterVotingYes_AndRestartAfterMasterHasCommitted_ConsultMasterAndRecoverDecision(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()

		self.masterProxy.commit(tid)

		self._startReplica1()
		self._startReplica2()


		self.assertEqual(value, self.masterProxy.get(key))

	def test_WhenReplicasDieAfterVotingYes_AndRestartAfterMasterHasAborted_ConsultMasterAndRecoverDecision(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()

		self.masterProxy.abort(tid)

		self._startReplica1()
		self._startReplica2()


		self.assertEqual(None, self.masterProxy.get(key))

	def test_WhenReplicasDieAfterVotingYes_AndRestartBeforeMasterHasDecided_RestoreTransactionAndWaitForDecision(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()

		self._startReplica1()
		self._startReplica2()

		self.masterProxy.commit(tid)

		self.assertEqual(value, self.masterProxy.get(key))
	
	def test_WhenReplicasDieAfterVotingYes_AndRestartBeforeMasterHasDecided_RestoreTransactionAndWaitForDecision_ButIfTimeoutsAfterNotReceivingFinalDecision_ConsultMaster(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()
		self._startReplica1()
		self._startReplica2()

		self.masterProxy.logCommit(tid)

		time.sleep(Replica.TIMEOUT)

		self.assertEqual(value, self.masterProxy.get(key))

	def test_WhenReplicasDieAfterVotingYesAndThenMasterDiesBeforeCommiting_MasterDecidesAbortOnRecovery(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()
		self._killMaster()

		self._startReplica1()
		self._startReplica2()
		self._startMaster()

		self.assertEqual(None, self.masterProxy.get(key))

	def test_WhenReplicasDieAfterVotingYesAndThenMasterDiesAfterLoggingCommit_RecoverMastersCommitDecision(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()

		self.masterProxy.logCommit(tid)
		self._killMaster()

		self._startReplica1()
		self._startReplica2()

		self._startMaster()

		self.assertEqual(value, self.masterProxy.get(key))

	def test_WhenReplicasDieAfterVotingYesAndThenMasterDiesAfterLoggingAbort_RecoverMastersAbortDecision(self):
		key = "somekey"
		value = "somevalue"
		tid = self.masterProxy.startPut(key, value)
		self.masterProxy.execute(tid)
		self.masterProxy.requestVotes(tid)

		self._killReplica1()
		self._killReplica2()

		self.masterProxy.logAbort(tid)
		self._killMaster()

		self._startReplica1()
		self._startReplica2()

		self._startMaster()

		self.assertEqual(None, self.masterProxy.get(key))


	
	def tearDown(self):
		self._cleanup()
	
	def _startMaster(self):
		print("Starting master mock")
		self.masterProcess = subprocess.Popen("startmastermock.py {0} {1} {2}".format(self.masterLogFile, self.replica1Port, self.replica2Port), shell=True)
		time.sleep(1)
