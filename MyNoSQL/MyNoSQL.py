import uuid
import json
import lzma
import os
import datetime
import time
import shutil

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer, HTTPServer
import urllib.parse
import tempfile
import inspect


class MyNoSQLServer(BaseHTTPRequestHandler):

	db = None

	def do_HEAD(self):
		return

	def do_POST(self):
		threadstart = time.time()
		httpcode = 200
		httpcode = 404
		message = "Unrecognised request: '{}'".format(parsed_path)
		self.send_response(httpcode)
		self.end_headers()
		self.wfile.write(bytes(message,"utf-8"))
		threadend = time.time()
		# base.debugmsg(5, parsed_path.path, "	threadstart:", "%.3f" % threadstart, "threadend:", "%.3f" % threadend, "Time Taken:", "%.3f" % (threadend-threadstart))
		base.debugmsg(5, "%.3f" % (threadend-threadstart), "seconds for ", parsed_path.path)
		return

	def do_GET(self):

		threadstart = time.time()
		httpcode = 404
		message = "Not Found"
		try:
			parsed_path = urllib.parse.urlparse(self.path)
			self.db.debugmsg(5, "parsed_path:", parsed_path)
			patharr = parsed_path.path.split("/")
			self.db.debugmsg(5, "patharr:", patharr)
			if (patharr[1] in ["peer", "index", "doc"]):
				httpcode = 200
				message = "OK"
				if patharr[1] == "peer":
					doc = self.db.getselfdoc()
					self.db.debugmsg(5, "doc:", doc)
					message = json.dumps(doc).encode("utf8")

			else:
				httpcode = 404
				message = "Unrecognised request: '{}'".format(parsed_path)
		except Exception as e:
			self.db.debugmsg(5, "do_GET:", e)
			httpcode = 500
			message = str(e)
		self.send_response(httpcode)
		self.end_headers()
		self.wfile.write(bytes(message,"utf-8"))
		threadend = time.time()
		# base.debugmsg(5, parsed_path.path, "	threadstart:", "%.3f" % threadstart, "threadend:", "%.3f" % threadend, "Time Taken:", "%.3f" % (threadend-threadstart))
		self.db.debugmsg(5, "%.3f" % (threadend-threadstart), "seconds for ", parsed_path.path)
		return

	def handle_http(self):
		return
	def respond(self):
		return

	# 	log_request is here to stop BaseHTTPRequestHandler logging to the console
	# 		https://stackoverflow.com/questions/10651052/how-to-quiet-simplehttpserver/10651257#10651257
	def log_request(self, code='-', size='-'):
		pass

	def setdb(self, db):
		self.db = db


class MyNoSQL:
	version = "0.0.1"
	debuglvl = 7

	# dbopen = False

	def __init__(self):
		# if not self.dbopen:
		self.db = {}
		self.selfurl = None
		self.doc_id = None
		self.port = 0
		self.dbopen = False
		# self.dblocation = os.path.dirname(os.path.realpath(__file__))
		self.dblocation = tempfile.gettempdir()


	def debugmsg(self, lvl, *msg):
		msglst = []
		prefix = ""
		suffix = ""
		# print("self.debuglvl:",self.debuglvl," >= lvl",lvl,"	", self.debuglvl >= lvl)
		if self.debuglvl >= lvl:
			try:
				# print("self.debuglvl:",self.debuglvl," >= 4","	", self.debuglvl >= 4)
				if self.debuglvl >= 4:
					stack = inspect.stack()
					the_class = stack[1][0].f_locals["self"].__class__.__name__
					the_method = stack[1][0].f_code.co_name
					the_line = stack[1][0].f_lineno
					prefix = "{}: {}({}): [{}:{}]	".format(str(the_class), the_method, the_line, self.debuglvl, lvl)
					if len(prefix.strip())<32:
						prefix = "{}	".format(prefix)
					if len(prefix.strip())<24:
						prefix = "{}	".format(prefix)

					msglst.append(str(prefix))

					suffix = "	[{} @{}]".format(self.version, str(datetime.datetime.now().isoformat(sep=' ', timespec='seconds')))

				print("msg:",msg)
				for itm in msg:
					msglst.append(str(itm))
				msglst.append(str(suffix))
				print(" ".join(msglst))
			except Exception as e:
				print("e:",e)
				pass



	def setdblocation(self, location):
		if os.path.isdir(location):
			self.dblocation = location
			return True
		else:
			raise Exception("location:", location, "does not exist or is not a folder")
			return False

	def _checkdb(self):
		if "dbpath" not in self.db:
			raise Exception("db not opened")
			return False
		return True

	def _servedb(self):

		# try to open a port in range 8800 - 8899
		for i in range(99):
			portno = 8800+i
			self.debugmsg(0, "trying port:", portno)
			server_address = ('', portno)
			reason = ""
			if self.dbopen:
				try:
					self.httpserver = ThreadingHTTPServer(server_address, MyNoSQLServer)
					self.port = portno
				except PermissionError:
					reason = "PermissionError"
				except Exception as e:
					reason = "{}".format(e)
			else:
				reason = "DB Closed"
			if self.port>0:
				break
			else:
				self.debugmsg(0, "open port failed:", reason)
		if self.port>0:
			self.debugmsg(0, "Server started on port:", self.port)
			self.httpserver.serve_forever()
		else:
			self.debugmsg(0, "Unable to start server:", reason)

	def _server(self):

		self.dbserver = threading.Thread(target=self._servedb)
		self.dbserver.start()

		peers = threading.Thread(target=self._findpeers)
		peers.start()

		while self.dbopen:
			reg = threading.Thread(target=self._registerself)
			reg.start()

			time.sleep(60)

		self.httpserver.shutdown()
		self.dbserver.join()

	def _findpeers(self):
		pass

	def addpeer(self, peerurl):
		pass

	def getselfdoc(self):
		if self.doc_id is not None:
			doc = self.readdoc(self.doc_id)
			return doc

	def _registerself(self):
		if self.port>0:
			doc = None
			if self.doc_id is None:
				srvdisphost = socket.gethostname()
				self.selfurl = "http://{}:{}".format(srvdisphost, self.port)
				dbservers = self.indexread("dbserver")
				if self.selfurl in list(dbservers.values()):
					# find doc id
					for dbserver in dbservers:
						self.debugmsg(5, "dbserver:", dbserver)
						self.debugmsg(5, "dbservers[dbserver]:", dbservers[dbserver])
						if dbservers[dbserver] == self.selfurl:
							self.doc_id = dbserver
							doc = self.readdoc(self.doc_id)
				else:
					# create new server doc
					doc = {}
					doc["dbserver"] = self.selfurl
					doc["altconn"] = []
			if doc is not None:
				t = datetime.datetime.now()
				doc["lastregistered"] = t.timestamp()
				self.savedoc(doc)

	def opendb(self, dbname):
		self.db["dbpath"] = os.path.join(self.dblocation, dbname)
		if not os.path.isdir(self.db["dbpath"]):
			os.mkdir(self.db["dbpath"])
			self.debugmsg(0, "DB Created:", self.db["dbpath"])
		self.index = os.path.join(self.db["dbpath"], "index")
		if os.path.isfile(self.index):
			self.db["index"] = self._loadindex()
			self.debugmsg(0, "DB Opened:", self.db["dbpath"])
		else:
			self.db["index"] = {}
			self.db["index"]["rev"] = {}
			self._saveindex()

		self.dbopen = True
		self.server = threading.Thread(target=self._server)
		self.server.start()

		# wait till serving
		while self.port<8800:
			time.sleep(1)



	def closedb(self):
		self.dbopen = False
		self.server.join()
		self.db = {}

	def _lockaquire(self, filename):
		timeout = 60
		t = datetime.datetime.now()
		timestart = t.timestamp()
		lockfile = "{}.lock".format(filename)
		while os.path.isfile(lockfile):
			timenow = t.timestamp()
			if (timenow - timestart)>timeout:
				return False
				break
			self.debugmsg(6, "waiting for lock on", filename)
			time.sleep(0.1)
		with open(lockfile, 'w') as f:
			f.write("{}".format(threading.get_native_id()))
			self.debugmsg(9, "lock aquired on", filename)
		return True

	def _lockrelease(self, filename):
		lockfile = "{}.lock".format(filename)
		if os.path.isfile(lockfile):
			os.remove(lockfile)
			self.debugmsg(9, "lock released on", filename)

	def _saveindex(self):
		if self._lockaquire(self.index):
			file = open(self.index, "wb")
			c_index = self._compressdata(self.db["index"])
			file.write(c_index)
			file.close()
			self._lockrelease(self.index)

	def _loadindex(self):
		if self._lockaquire(self.index):
			file = open(self.index, "rb")
			rawdata = file.read()
			file.close()
			self._lockrelease(self.index)
			self.debugmsg(9, "rawdata:", rawdata)
			data = self._decompressdata(rawdata)
			return data


	def _template0(self):
		pass

	def _compressdata(self, data):
		data_in = bytearray(json.dumps(data).encode("utf8"))
		data_out = lzma.compress(data_in)
		return data_out

	def _decompressdata(self, data_in):
		data_out = lzma.decompress(data_in)
		data = json.loads(data_out)
		return data

	def _generatedocid(self):
		return str(uuid.uuid1())

	def _updaterev(self, doc):
		irev = 0
		t = datetime.datetime.now()
		if "rev" in doc:
			srev = doc["rev"].split(".", 1)[0]
			irev = int(srev)
		doc["rev"] = "{}.{}".format(irev+1, t.timestamp())
		return doc

	def _checkrev(self, doc):
		if "id" not in doc:
			return True
		id = doc["id"]
		if not self._docindexed(id):
			return True
		if "rev" not in doc:
			return False
		odoc = self.readdoc(id)
		if doc["rev"] != odoc["rev"]:
			raise Exception("Document revisions don't match")
			return False

		return True


	def _indexdoc(self, doc):
		changed = False
		kidx = list(self.db["index"].keys())
		kidx.remove("local")
		for key in kidx:
			if key in doc:
				if doc["id"] not in self.db["index"][key] or self.db["index"][key][doc["id"]] != doc[key]:
					self.db["index"][key][doc["id"]] = doc[key]
					changed = True
		if changed:
			self._saveindex()

	def indexadd(self, index):
		if index not in self.db["index"]:
			self.db["index"][index] = {}
			self._saveindex()
		return True

	def indexdrop(self, index):
		if index in self.db["index"]:
			del self.db["index"][index]
			self._saveindex()
		return True

	def indexread(self, index):
		if index in self.db["index"]:
			return self.db["index"][index]
		else:
			self.indexadd(index)
			self.indexupdate()
			return self.db["index"][index]

	def indexupdate(self):
		docs = self.db["index"]["rev"].keys()
		for id in docs:
			self.readdoc(id)

	def _docindexed(self, doc_id):
		rev = self.indexread("rev")
		if doc_id in list(rev.keys()):
			return True
		return False


	def savedoc(self, doc):
		t = datetime.datetime.now()
		self._checkrev(doc)
		if "id" not in doc:
			doc["id"] = self._generatedocid()
		doc = self._updaterev(doc)
		if "documents" not in self.db:
			self.db["documents"] = {}
		if doc["id"] not in self.db["documents"]:
			self.db["documents"][doc["id"]] = {}
		self.db["documents"][doc["id"]]["data"] = doc
		self.db["documents"][doc["id"]]["accessed"] = t.timestamp()
		self._savetoshard(doc["id"])
		self._indexdoc(doc)
		return doc

	def readdoc(self, doc_id):
		doc = None
		if self._islocal(doc_id) is not None:
			t = datetime.datetime.now()
			if "documents" not in self.db:
				self.db["documents"] = {}
			if doc_id not in self.db["documents"]:
				# need to load the doc from the shard
				shard_id = self._getshardid(doc_id)
				if "shards" not in self.db:
					self.db["shards"] = {}
				if shard_id not in self.db["shards"]:
					self.db["shards"][shard_id] = self._loadshard(shard_id)
				self.db["documents"][doc_id] = {}
				self.db["documents"][doc_id]["data"] = self.db["shards"][shard_id][doc_id]
				self.db["documents"][doc_id]["accessed"] = t.timestamp()
				self._freeshard(shard_id)

			doc = self.db["documents"][doc_id]["data"]
			self.db["documents"][doc_id]["accessed"] = t.timestamp()

			self._indexdoc(doc)
		return doc

	def _getshardid(self, doc_id):
		# shard_id = doc_id.split("-")[0]
		# I have been comtemplating how big to make the shard_id
		# 	it's a compromise between having too many files or too big files.
		# 	Initially I was going to make the the first 2 char of the uuid
		# 	but then I made it the first part (8 char) which might make too many files?
		# Further thought 16 x 16 (2 char) = 256 files, this was the windows limit at
		# 	one stage. might still be?
		# Found the answer, FAT16 limit is 512 files in a folder and a 3 char shard
		# 	16 x 16 x 16 = 4096 would well exceed this, but would be fine on FAT32 or NTFS
		# 	https://stackoverflow.com/questions/4944709/windows-limit-on-the-number-of-files-in-a-particular-folder#14407078
		shard_id = doc_id[0:2]
		return shard_id

	def _islocal(self, doc_id):
		if "local" not in self.db["index"]:
			self.db["index"]["local"] = {}
		if doc_id in self.db["index"]["local"].keys():
			return self.db["index"]["local"][doc_id]

		shard_id = self._getshardid(doc_id)
		if "shards" not in self.db:
			self.db["shards"] = {}
		if shard_id not in self.db["shards"]:
			self.db["shards"][shard_id] = self._loadshard(shard_id)
			for id in list(self.db["shards"][shard_id].keys()):
				self.db["index"]["local"][id] = self.db["shards"][shard_id][id]["rev"]
			self._freeshard(shard_id)

		if doc_id in self.db["index"]["local"].keys():
			return self.db["index"]["local"][doc_id]

		return None


	def _savetoshard(self, doc_id):
		if "local" not in self.db["index"]:
			self.db["index"]["local"] = {}
		shard_id = self._getshardid(doc_id)
		self.debugmsg(7, "shard_id:", shard_id, "	doc_id:", doc_id)
		if "shards" not in self.db:
			self.db["shards"] = {}
		if shard_id not in self.db["shards"]:
			self.db["shards"][shard_id] = self._loadshard(shard_id)
		self.db["shards"][shard_id][doc_id] = self.db["documents"][doc_id]["data"]
		self.db["index"]["local"][doc_id] = self.db["shards"][shard_id][doc_id]["rev"]
		self._saveshard(shard_id)

	def _saveshard(self, shard_id):
		c_shard = self._compressdata(self.db["shards"][shard_id])
		f_shard = os.path.join(self.db["dbpath"], shard_id)
		if self._lockaquire(f_shard):
			file = open(f_shard, "wb")
			file.write(c_shard)
			file.close()
			self._lockrelease(f_shard)
			del self.db["shards"][shard_id]

	def _loadshard(self, shard_id):
		f_shard = os.path.join(self.db["dbpath"], shard_id)
		data = {}
		if os.path.isfile(f_shard):
			if self._lockaquire(f_shard):
				file = open(f_shard, "rb")
				rawdata = file.read()
				file.close()
				self._lockrelease(f_shard)
				data = self._decompressdata(rawdata)
		return data

	def _freeshard(self, shard_id):
		del self.db["shards"][shard_id]
