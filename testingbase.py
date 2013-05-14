import unittest
import subprocess
import xmlrpc.client
import os
import time

class TestingBase(unittest.TestCase):
	def _initialize(self):
		self.masterLogFile = "test-master-log.txt"

		self.replica1LogFile = "test-replica1-log.txt"
		self.replica1Port = 8888
		self.replica1DbName = "test-replica1-db"

		self.replica2LogFile = "test-replica2-log.txt"
		self.replica2Port = 9999
		self.replica2DbName = "test-replica2-db"

		self._removeFile(self.masterLogFile)	
		self._removeFile(self.replica1LogFile)	
		self._removeFile(self.replica2LogFile)	

		self._removeDb(self.replica1DbName)
		self._removeDb(self.replica2DbName)

		self._startReplica1()
		self._startReplica2()
		self._startMaster()

		self.masterProxy = xmlrpc.client.ServerProxy("http://localhost:8000")
		self.replica1Proxy = xmlrpc.client.ServerProxy("http://localhost:8888")
		self.replica2Proxy = xmlrpc.client.ServerProxy("http://localhost:9999")
		print("setUP Done")


	def _cleanup(self):
		self._killMaster()
		self._killReplica1()
		self._killReplica2()

	def _createFile(self, fileName):
		f = open(fileName, "w")
		f.close()

	def _removeDb(self, fileName):
		self._removeFile(fileName + ".dat")
		self._removeFile(fileName + ".bak")
		self._removeFile(fileName + ".dir")

	def _removeFile(self, fileName):
		if os.path.isfile(fileName):
			os.remove(fileName)
			time.sleep(1)
	
	def _startMaster(self):
		print("Starting master")
		self.masterProcess = subprocess.Popen("startmaster.py {0} {1} {2}".format(self.masterLogFile, self.replica1Port, self.replica2Port), shell=True)
		time.sleep(1)

	def _startReplica1(self):
		print("Starting replica 1")
		self.replica1Process = subprocess.Popen("startreplica.py {0} {1} {2}".format(self.replica1LogFile, self.replica1Port, self.replica1DbName), shell=True)
		time.sleep(1)

	def _startReplica2(self):
		print("Starting replica 2")
		self.replica2Process = subprocess.Popen("startreplica.py {0} {1} {2}".format(self.replica2LogFile, self.replica2Port, self.replica2DbName), shell=True)
		time.sleep(1)


	def _killMaster(self):
		print("Killing master")
		subprocess.Popen("taskkill /F /T /PID %i"%self.masterProcess.pid , shell=True)
		time.sleep(1)
		#self.masterProcess.terminate()

	def _killReplica1(self):
		print("Killing replica 1")
		subprocess.Popen("taskkill /F /T /PID %i"%self.replica1Process.pid , shell=True)
		time.sleep(1)
		#self.replica1Process.terminate()

	def _killReplica2(self):
		print("Killing rpelica 2")
		subprocess.Popen("taskkill /F /T /PID %i"%self.replica2Process.pid , shell=True)
		time.sleep(1)
		#self.replica2Process.terminate()
