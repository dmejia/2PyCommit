import unittest
import subprocess
import xmlrpc.client
import os
import time
import testingbase

class BasicTests(testingbase.TestingBase):
	def setUp(self):
		self._initialize()

	def test_putAndGet(self):
		key = "somekey"
		value = "somevalue"
		self.masterProxy.put(key, value)
		self.assertEqual(value, self.masterProxy.get(key))

	def test_putPutAndGet(self):
		key = "somekey"
		value1 = "somevalue"
		value2 = "someothervalue"
		self.masterProxy.put(key, value1)
		self.masterProxy.put(key, value2)
		self.assertEqual(value2, self.masterProxy.get(key))

	def test_putDeleteAndGet(self):
		key = "somekey"
		value = "somevalue"
		self.masterProxy.put(key, value)
		self.masterProxy.delete(key)
		self.assertEqual(None, self.masterProxy.get(key))

	# put, then kill and restart replicas, and then get
	def test_replicaStoresArePersistent(self):
		key = "somekey"
		value = "somevalue"
		self.masterProxy.put(key, value)

		self._killReplica1()
		self._killReplica2()
		self._startReplica1()
		self._startReplica2()

		self.assertEqual(value, self.masterProxy.get(key))

	# put, then kill replicas and their dbs, restart replicas, and then get
	def test_basicReplicaRecoveryWithPut(self):
		key = "somekey"
		value = "somevalue"
		self.masterProxy.put(key, value)

		self._killReplica1()
		self._killReplica2()
		self._removeDb(self.replica1DbName)
		self._removeDb(self.replica2DbName)
		self._startReplica1()
		self._startReplica2()

		self.assertEqual(value, self.masterProxy.get(key))

	# put, then delete then kill replicas and their dbs, restart replicas, and then get
	def test_basicReplicaRecoveryWithDelete(self):
		key = "somekey"
		value = "somevalue"
		self.masterProxy.put(key, value)
		self.masterProxy.delete(key)

		self._killReplica1()
		self._killReplica2()
		self._removeDb(self.replica1DbName)
		self._removeDb(self.replica2DbName)
		self._startReplica1()
		self._startReplica2()

		self.assertEqual(None, self.masterProxy.get(key))

	# put, then kill master, and then get (kind of useless because the replicas are fine, but just for sanity checking the code path)
	def test_basicMasterRecovery(self):
		key = "somekey"
		value = "somevalue"
		self.masterProxy.put(key, value)

		self._killMaster()
		self._startMaster()

		self.assertEqual(value, self.masterProxy.get(key))

	def test_withOneReplicaDown_MasterAbortsPut(self):
		key = "somekey"
		value = "somevalue"

		self._killReplica1()

		committed = self.masterProxy.put(key, value)
		self.assertFalse(committed)

		self._startReplica1()

		self.assertEqual(None, self.masterProxy.get(key))

	def test_withOneReplicaDown_MasterIsAbleToServeGets(self):
		key = "somekey"
		value = "somevalue"
		for i in range(5):
			self.masterProxy.put(key + str(i), value + str(i))

		self._killReplica1()

		for i in range(5):
			self.assertEqual(value + str(i), self.masterProxy.get(key + str(i)))

	def test_whenAllReplicasAreDown_MasterTrowsOnGet(self):
		key = "somekey"
		value = "somevalue"


		self.masterProxy.put(key, value)

		self._killReplica1()
		self._killReplica2()

		with self.assertRaises(Exception):
			self.masterProxy.get(key)


	def tearDown(self):
		self._cleanup()

