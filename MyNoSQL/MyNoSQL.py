import uuid
import json
import lzma
import os
import datetime
import time
import shutil
import random

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer, HTTPServer
import urllib.parse
import tempfile
import inspect
import hashlib
import requests

db = None

ONE_MINUTE = 60
ONE_HOUR = 60 * ONE_MINUTE
ONE_DAY = 24 * ONE_HOUR
ONE_WEEK = 7 * ONE_DAY
ONE_MONTH = 4 * ONE_WEEK
ONE_YEAR = 52 * ONE_WEEK


class MyNoSQLServer(BaseHTTPRequestHandler):

	# def __init__(self):
	# 	self.logger = Logger()

	def do_OPTIONS(self):
		self.send_response(200, "ok")
		self.send_header('Access-Control-Allow-Origin', '*')
		self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
		self.send_header("Access-Control-Allow-Headers", "X-Requested-With")
		self.send_header("Access-Control-Allow-Headers", "Content-Type")
		self.end_headers()

	def do_HEAD(self):
		return

	def do_POST(self):

		print("")
		print("--------------- <do_POST> ---------------")
		threadstart = time.time()
		# default result
		httpcode = 404
		try:
			parsed_path = urllib.parse.urlparse(self.path)
			db.debugmsg(5, "parsed_path:", parsed_path)
			patharr = parsed_path.path.split("/")
			db.debugmsg(5, "patharr:", patharr)

			message = "Unrecognised request: '{}'".format(parsed_path)

			if (patharr[1].lower() in ["peer", "doc"]):
				httpcode = 200
				message = "OK"

				db.debugmsg(5, "patharr[1].lower():", patharr[1].lower())

				if patharr[1].lower() == "peer":
					content_len = int(self.headers.get('Content-Length'))
					post_body = self.rfile.read(content_len)
					doc = json.loads(post_body)

					db.debugmsg(5, "doc:", doc)

					db.debugmsg(5, "db:", db)
					db.debugmsg(5, "db.doc_id:", db.doc_id)

					if "id" in doc:
						db._registerpeer(doc["id"])

						saved = db._saveremotedoc(doc)
						db.debugmsg(5, "saved:", saved)

				if patharr[1].lower() == "doc":

					content_len = int(self.headers.get('Content-Length'))
					post_body = self.rfile.read(content_len)
					doc = json.loads(post_body)

					db.debugmsg(5, "doc:", doc)

					db.debugmsg(5, "db:", db)
					db.debugmsg(5, "db.doc_id:", db.doc_id)

					if "id" in doc:

						saved = db._saveremotedoc(doc)
						db.debugmsg(5, "saved:", saved)



		except Exception as e:
			db.debugmsg(5, "e:", e)
			httpcode = 500
			message = str(e)

		db.debugmsg(5, "httpcode:", httpcode, "	message:", message)
		self.send_response(httpcode)
		self.end_headers()
		self.wfile.write(bytes(message,"utf-8"))
		threadend = time.time()
		# base.debugmsg(5, parsed_path.path, "	threadstart:", "%.3f" % threadstart, "threadend:", "%.3f" % threadend, "Time Taken:", "%.3f" % (threadend-threadstart))
		db.debugmsg(5, "%.3f" % (threadend-threadstart), "seconds for ", parsed_path.path)
		print("--------------- </do_POST> ---------------")
		print("")
		return

	def do_GET(self):

		print("")
		print("--------------- <do_GET> ---------------")
		threadstart = time.time()
		httpcode = 404
		message = "Not Found"
		try:
			parsed_path = urllib.parse.urlparse(self.path)
			db.debugmsg(5, "parsed_path:", parsed_path)
			patharr = parsed_path.path.split("/")
			db.debugmsg(5, "patharr:", patharr)
			if (patharr[1].lower() in ["peer", "index", "doc"]):
				httpcode = 200
				message = "OK"
				if patharr[1].lower() == "peer":
					db.debugmsg(5, "db:", db)
					doc = db.getselfdoc()
					db.debugmsg(5, "doc:", doc)
					# message = json.dumps(doc).encode("utf8")
					message = json.dumps(doc)

				if patharr[1].lower() == "index":
					if len(patharr)>2:
						if patharr[2] in db._indexlist():
							message = json.dumps(db.indexread(patharr[2]))
						else:
							httpcode = 404
							message = "Index Not Found"
					else:
						message = json.dumps(db._indexlist())


				if patharr[1].lower() == "doc":
					if len(patharr)>2:
						doc = db.readdoc(patharr[2])
						if doc is not None:
							message = json.dumps(doc)
						else:
							httpcode = 404
							message = "Document not found"
					else:
						httpcode = 404
						message = "Document ID Required"

			else:
				httpcode = 404
				message = "Unrecognised request: '{}'".format(parsed_path)
		except Exception as e:
			db.debugmsg(5, "e:", e)
			httpcode = 500
			message = str(e)
		self.send_response(httpcode)
		# // Access-Control-Allow-Origin: *
		self.send_header("Access-Control-Allow-Origin", "*")
		self.end_headers()
		self.wfile.write(bytes(message,"utf-8"))
		threadend = time.time()
		# base.debugmsg(5, parsed_path.path, "	threadstart:", "%.3f" % threadstart, "threadend:", "%.3f" % threadend, "Time Taken:", "%.3f" % (threadend-threadstart))
		db.debugmsg(5, "%.3f" % (threadend-threadstart), "seconds for ", parsed_path.path)
		print("--------------- </do_GET> ---------------")
		print("")
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
	version = "0.0.6"
	debuglvl = 7
	timeout=600
	defaultspeed=999999999

	# dbopen = False

	def __init__(self):
		# if not self.dbopen:
		global db
		self.db = {}
		self.selfurl = None
		self.doc_id = None
		self.port = 0
		self.dbopen = False
		# self.dblocation = os.path.dirname(os.path.realpath(__file__))
		self.dblocation = tempfile.gettempdir()
		db = self

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

				# print("msg:",msg)
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
					# self.httpserver.setdb(self)
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
			self.doc_id = self._registerself()
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
			if self.dbopen:
				reg = threading.Thread(target=self._registerself)
				reg.start()

			for i in range(60):
				if self.dbopen:
					time.sleep(1)

			if self.dbopen:
				upd = threading.Thread(target=self._peerupdates)
				upd.start()


		self.httpserver.shutdown()
		self.dbserver.join()

	def _sortpeerspeed(self, e):
	  return e['speed']

	def _registerpeer(self, doc_id):
		if "peers" not in self.db:
			self.db["peers"] = {}
		if "sorted" not in self.db["peers"]:
			self.db["peers"]["sorted"] = []
		if "speed" not in self.db["peers"]:
			self.db["peers"]["speed"] = {}

		if doc_id not in self.db["peers"]["sorted"]:
			# self.db["peers"]["sorted"].append(doc_id)
			self.db["peers"]["sorted"].append({'id':doc_id,'speed':self.defaultspeed})
			self.db["peers"]["speed"][doc_id] = self.defaultspeed

			peerspeed = threading.Thread(target=self._getpeerspeed, args=(doc_id,))
			peerspeed.start()

	def _getpeerspeed(self, doc_id):
		self.debugmsg(5, "doc_id:", doc_id)
		peerdoc = self.readdoc(doc_id)
		self.debugmsg(5, "peerdoc:", peerdoc)

		t = datetime.datetime.now()
		tstart = t.timestamp()
		peerdata = self._getremote(peerdoc["dbserver"] + "/peer")
		t = datetime.datetime.now()
		tend = t.timestamp()
		speed = tend - tstart
		# self.db["peers"].append({'id':doc_id,'speed':speed)
		self.db["peers"]["speed"][doc_id] = speed
		self._sortpeerspeeds()

	def _getallpeerspeeds(self):
		for peer in self.db["peers"]["sorted"]:
			peerspeed = threading.Thread(target=self._getpeerspeed, args=(peer['id'],))
			peerspeed.start()

	def _sortpeerspeeds(self):
		peerssorted = []
		for doc_id in self.db["peers"]["speed"].keys():
			peerssorted.append({'id':doc_id,'speed':self.db["peers"]["speed"][doc_id]})

		self.db["peers"]["sorted"] = peerssorted
		self.db["peers"]["sorted"].sort(key=self._sortpeerspeed)

	def _findpeers(self):
		time.sleep(5)
		dbservers = self.indexread("dbserver")
		self.debugmsg(5, "dbservers:", dbservers)
		for dbserver in dbservers:
			self.debugmsg(5, "dbserver:", dbserver)
			self.debugmsg(5, "dbservers[dbserver]:", dbservers[dbserver])
			if dbserver != self.doc_id:
				# indexes = self._getremote(dbservers[dbserver] + "/Index")
				# self.debugmsg(5, "indexes:", indexes)
				dbserverdoc = self.readdoc(dbserver)
				if dbserverdoc["dbmode"] != "Peer":
					self._registerpeer(dbserver)

	def _haspeers(self):
		if "peers" not in self.db:
			return False
		if "sorted" not in self.db["peers"]:
			return False

		self.debugmsg(9, "self.db[peers]:", len(self.db["peers"]["sorted"]), self.db["peers"]["sorted"])
		if len(self.db["peers"]["sorted"]) > 0:
			return True

		return False

	def _choosepeer(self):
		peerid = None
		if self._haspeers():
			self._sortpeerspeeds()
			fastest = self.db["peers"]["sorted"][0]
			if fastest['speed'] == self.defaultspeed:
				mirror = random.choice(self.db["peers"]["sorted"])
				peerid = mirror['id']
			else:
				peerid = fastest['id']
		return peerid

	def _peerupdates(self):
		if self.dbopen:
			if self._haspeers():
				peerspeed = threading.Thread(target=self._getallpeerspeeds)
				peerspeed.start()

				selfdoc = self.getselfdoc()
				self.debugmsg(9, "selfdoc:", selfdoc)
				if selfdoc["dbmode"] == "Peer":
					mirrorid = self._choosepeer()
					self._getpeerupdates(mirrorid, selfdoc["dbmode"])
				# if selfdoc["dbmode"] == "Mirror":
				if selfdoc["dbmode"] != "Peer":
					for peer in self.db["peers"]["sorted"]:
						self._getpeerupdates(peer['id'], selfdoc["dbmode"])

	def _getpeerupdates(self, doc_id, mode):
		self.debugmsg(8, "doc_id:", doc_id)
		peerdoc = self.readdoc(doc_id)
		if "dbserver" in peerdoc:
			peerindexes = self._getremote(peerdoc["dbserver"] + "/Index")
			if peerindexes is not None:
				self.debugmsg(8, "peerindexes:", peerindexes)
				for index in peerindexes:
					self._updatepeerindex(peerdoc["dbserver"], peerdoc["dbmode"], index)
		# if mode == "Mirror":
		if mode != "Peer":
			peerrevs = self._getremote(peerdoc["dbserver"] + "/Index/rev")
			self.debugmsg(8, "peerrevs:", peerrevs)
			if peerrevs is not None:
				t = datetime.datetime.now()
				for peerrev in peerrevs:
					rdet = self._revdetail(peerrevs[peerrev])
					self.debugmsg(8, "rdet:", rdet)
					getremote = False
					if rdet["epoch"] > (t.timestamp() - ONE_YEAR):
						islocal = self._islocal(peerrev)
						if islocal is None:
							getremote = True
						else:
							ldet = self._revdetail(islocal)
							if rdet["number"] > ldet["number"]:
								getremote = True

						self.debugmsg(8, "getremote:", getremote)
						if getremote:
							self.debugmsg(8, "_getremote url:", peerdoc["dbserver"] + "/Doc/" + peerrev)
							rdoc = self._getremote(peerdoc["dbserver"] + "/Doc/" + peerrev)
							self.debugmsg(8, "rdoc:", rdoc)
							self._saveremotedoc(rdoc)


	def _updatepeerindex(self, peerurl, mode, index):
		self.debugmsg(8, "index:", index)
		indexremote = self._getremote(peerurl + "/Index/" + index)
		self.debugmsg(8, "index:", index, "indexremote:", indexremote)
		indexlocal = self.indexread(index)
		self.debugmsg(8, "index:", index, "indexlocal:", indexlocal)
		for item in indexremote:
			self.debugmsg(8, "item:", item)
			if item not in indexlocal.keys():
				self._indexadd(index, item, indexremote[item])
				indexlocal = self.indexread(index)
			if index == "rev":
				# compare revisions
				try:
					rdet = self._revdetail(indexremote[item])
				except:
					doc = self.readdoc(indexremote[item])
					doc = self._updaterev(doc)
					doc = self.savedoc(doc)
					rdet = self._revdetail(indexremote[item])

				self.debugmsg(8, "rdet:", rdet)
				ldet = self._revdetail(indexlocal[item])
				self.debugmsg(8, "ldet:", ldet)
				if rdet["number"] > ldet["number"]:
					self._indexadd(index, item, indexremote[item])

		if mode != "Peer":
			indexlocal = self.indexread(index)
			indexremote = self._getremote(peerurl + "/Index/" + index)
			for item in indexlocal:
				self.debugmsg(8, "item:", item)
				if item not in indexremote.keys():
					ldoc = self.readdoc(item)
					self.debugmsg(8, "_sendremote url:", peerurl + "/Doc")
					self._sendremote(peerurl + "/Doc", ldoc)
					indexremote = self._getremote(peerurl + "/Index/" + index)
				if index == "rev":
					# compare revisions
					rdet = self._revdetail(indexremote[item])
					self.debugmsg(8, "rdet:", rdet)
					ldet = self._revdetail(indexlocal[item])
					self.debugmsg(8, "ldet:", ldet)
					if ldet["number"] > rdet["number"]:
						ldoc = self.readdoc(item)
						self.debugmsg(5, "_sendremote url:", peerurl + "/Doc")
						self._sendremote(peerurl + "/Doc", ldoc)


	def _getremote(self, uri):
		try:
			r = requests.get(uri, timeout=self.timeout)
			self.debugmsg(9, "resp: ", r.status_code, "r.text:", r.text)
			if (r.status_code != requests.codes.ok):
				self.debugmsg(9, "r.status_code:", r.status_code, "!=", requests.codes.ok)
				return None
			else:
				if "{" in r.text or "[" in r.text:
					jsonresp = json.loads(r.text)
					self.debugmsg(9, "jsonresp: ", jsonresp)
					return jsonresp
				else:
					return r.text

		except Exception as e:
			self.debugmsg(8, "Exception:", e)
			return None

	def _sendremote(self, uri, payload):
		try:
			r = requests.post(uri, json=payload, timeout=self.timeout)
			self.debugmsg(9, "resp: ", r.status_code, r.text)
			if (r.status_code != requests.codes.ok):
				self.debugmsg(5, "	r.status_code:", r.status_code, "!=", requests.codes.ok)
				self.debugmsg(5, "	uri: ", uri, "json", payload)
				self.debugmsg(5, "	resp: ", r.status_code, r.text)
				return None
			else:
				if "{" in r.text or "[" in r.text:
					jsonresp = json.loads(r.text)
					self.debugmsg(9, "jsonresp: ", jsonresp)
					return jsonresp
				else:
					return r.text

		except Exception as e:
			self.debugmsg(8, "Exception:", e)
			return None

	def addpeer(self, peerurl):
		uri = peerurl + "/Peer"
		# payload = {
		# 	"AgentName": self.agentname,
		# 	"Action": "Status",
		# 	"Hash": hash
		# }
		payload = self.getselfdoc()
		self.debugmsg(9, "payload: ", payload)
		resp = self._sendremote(uri, payload)
		self.debugmsg(5, "resp: ", resp)
		peerdoc = self._getremote(uri)
		self.debugmsg(5, "peerdoc: ", peerdoc)
		if peerdoc is not None and "id" in peerdoc:
			self.debugmsg(5, "_saveremotedoc(peerdoc): ", peerdoc)
			self._saveremotedoc(peerdoc)
			if "dbmode" in peerdoc and peerdoc["dbmode"] == "Mirror":
				self._registerpeer(peerdoc["id"])
		# pass

	def getselfdoc(self):
		if self.dbopen:
			if self.doc_id is None:
				self.doc_id = self._registerself()
			if self.doc_id is not None:
				doc = self.readdoc(self.doc_id)
				return doc
			else:
				raise Exception("self.doc_id is None")
				return None
		else:
			raise Exception("DB not open.")
			return None

	def _registerself(self):
		if self.port>0:
			doc = None
			if self.doc_id is None:
				srvdisphost = socket.gethostname()
				self.selfurl = "http://{}:{}".format(srvdisphost, self.port)
				self.debugmsg(7, "self.selfurl:", self.selfurl)
				dbservers = self.indexread("dbserver")
				self.debugmsg(7, "dbservers:", dbservers)

				if self.selfurl in list(dbservers.values()):
					# find doc id
					for dbserver in dbservers:
						self.debugmsg(5, "dbserver:", dbserver)
						self.debugmsg(5, "dbservers[dbserver]:", dbservers[dbserver])
						if dbservers[dbserver] == self.selfurl:
							self.doc_id = dbserver
							self.debugmsg(7, "self.doc_id:", self.doc_id)

					doc = self.readdoc(self.doc_id)
					self.debugmsg(7, "doc:", doc)
					if doc is None:
						raise Exception("doc is None.", doc)

				else:
					# create new server doc
					doc = {}
					doc["dbserver"] = self.selfurl
					doc["altconn"] = []


				self.debugmsg(7, "doc:", doc)
				if "dbmode" not in doc:
					doc["dbmode"] = "Peer"

			if doc is not None:
				t = datetime.datetime.now()
				doc["lastregistered"] = t.timestamp()
				self.savedoc(doc)
			return self.doc_id

	def getdbmode(self):
		if self.dbopen:
			doc = self.getselfdoc()
			if "dbmode" in doc:
				return doc["dbmode"]
			else:
				return None
		else:
			return None

	def setdbmode(self, newmode):
		# check for supported modes
		self.debugmsg(5, "newmode:", newmode)
		if newmode not in ["Peer", "Mirror"]:
			return False
		if self.dbopen:
			doc = self.getselfdoc()
			if "dbmode" not in doc:
				doc["dbmode"] = newmode
				self.savedoc(doc)
			else:
				if doc["dbmode"] is not newmode:
					doc["dbmode"] = newmode
					self.savedoc(doc)
			self.debugmsg(7, "doc:", doc)
			return True
		else:
			return False

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
		self.debugmsg(0, "Closing DB:", self.db["dbpath"])
		self.dbopen = False
		self.server.join()
		self.db = {}
		self.debugmsg(0, "DB Closed")

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
			try:
				os.remove(lockfile)
				self.debugmsg(9, "lock released for", filename)
			except:
				self.debugmsg(9, "lock already released for", filename)

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

		ts = t.timestamp()
		tss = int(ts)
		tsm = int("{}".format(ts - tss).split(".")[1][0:6])
		doc["rev"] = "{}.{:x}.{:x}".format(irev+1, tss, tsm)
		self.debugmsg(8, "doc[rev]:", doc["rev"])
		return doc

	def _revdetail(self, rev):
		detail = {}
		detail["string"] = rev
		if "." not in rev:
			rev += ".0.0"
		arev = rev.split(".")
		detail["number"] = int(arev[0])

		# hex_val = 'beef101'
		# print(int(hex_val, 16))
		detail["epoch"] = float("{}.{}".format(int(arev[1], 16), int(arev[2], 16)))

		return detail

	def _checkrev(self, doc):
		if "id" not in doc:
			return True
		id = doc["id"]
		if not self._docindexed(id):
			return True
		if "rev" not in doc:
			return False
		if self._islocal(id) is None:
			return True
		odoc = self.readdoc(id)
		if doc["rev"] != odoc["rev"]:
			cdet = self._revdetail(doc["rev"])
			# self.debugmsg(5, "cdet:", cdet)
			odet = self._revdetail(odoc["rev"])
			# self.debugmsg(5, "odet:", odet)
			# self.debugmsg(5, "type(odet[epoch]):", type(odet["epoch"]))	type(odet[epoch]): <class 'float'>
			if cdet["number"] > odet["number"]:
				return True

			raise Exception("Document revisions don't match:", doc["rev"], "!=", odoc["rev"])
			return False

		return True

	def _indexadd(self, key, doc_id, value):
		changed = False
		self.debugmsg(9, "key:", key)
		if key not in self._indexlist():
			self.indexadd(key)
		self.debugmsg(9, "doc_id:", doc_id)

		self.debugmsg(7, "doc_id:", doc_id, "	key:", self.db["index"][key], "	index:", self.db["index"])
		if doc_id not in self.db["index"][key].keys():
			self.db["index"][key][doc_id] = value
			changed = True
		self.debugmsg(9, "value:", value)
		if self.db["index"][key][doc_id] != value:
			self.db["index"][key][doc_id] = value
			changed = True

		self.debugmsg(9, "changed:", changed)
		if changed:
			self._saveindex()

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

	def _indexlist(self):
		idxlst = list(self.db["index"].keys())
		# self.debugmsg(9, "idxlst:", idxlst)
		return idxlst

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

	def _dochash(self, doc):
		hasher = hashlib.md5()
		# hasher.update(str(os.path.getmtime(file)).encode('utf-8'))
		hasher.update(json.dumps(doc).encode("utf8"))
		return hasher.hexdigest()

	def _saveremotedoc(self, doc):
		t = datetime.datetime.now()
		self._checkrev(doc)
		if "id" not in doc:
			doc["id"] = self._generatedocid()
		doc_id = doc["id"]
		if "documents" not in self.db:
			self.db["documents"] = {}
		if doc["id"] not in self.db["documents"]:
			self.db["documents"][doc["id"]] = {}

		if "dochash" not in self.db["documents"][doc["id"]] or \
			self.db["documents"][doc["id"]]["dochash"] != self._dochash(doc):
			self.db["documents"][doc["id"]]["data"] = doc
			self.db["documents"][doc["id"]]["accessed"] = t.timestamp()
			self.db["documents"][doc["id"]]["dochash"] = self._dochash(doc)
			self._savetoshard(doc["id"])
			self._indexdoc(doc)
		return doc

	def savedoc(self, doc):
		t = datetime.datetime.now()
		self._checkrev(doc)
		if "id" not in doc:
			doc["id"] = self._generatedocid()
		doc_id = doc["id"]
		if "documents" not in self.db:
			self.db["documents"] = {}
		if doc["id"] not in self.db["documents"]:
			self.db["documents"][doc["id"]] = {}

		if "dochash" not in self.db["documents"][doc["id"]] or \
			self.db["documents"][doc["id"]]["dochash"] != self._dochash(doc):

			doc = self._updaterev(doc)
			self.db["documents"][doc["id"]]["data"] = doc
			self.db["documents"][doc["id"]]["accessed"] = t.timestamp()
			self.db["documents"][doc["id"]]["dochash"] = self._dochash(doc)
			self._savetoshard(doc["id"])
			self._indexdoc(doc)

			if self.dbopen:
				upd = threading.Thread(target=self._peerupdates)
				upd.start()

		return doc


	def readdoc(self, doc_id):
		doc = None
		islocal = self._islocal(doc_id)
		self.debugmsg(8, "doc_id:", doc_id, "islocal:", islocal)
		t = datetime.datetime.now()
		if "documents" not in self.db:
			self.db["documents"] = {}
		if islocal is not None:
			if doc_id not in self.db["documents"]:
				# need to load the doc from the shard
				shard_id = self._getshardid(doc_id)
				if "shards" not in self.db:
					self.db["shards"] = {}
				if shard_id not in self.db["shards"]:
					self.db["shards"][shard_id] = self._loadshard(shard_id)
				self.db["documents"][doc_id] = {}
				if doc_id in self.db["shards"][shard_id]:
					self.db["documents"][doc_id]["data"] = self.db["shards"][shard_id][doc_id]
				else:
					raise Exception("doc_id",doc_id,"not found", doc)

				self.db["documents"][doc_id]["accessed"] = t.timestamp()
				self.db["documents"][doc_id]["dochash"] = self._dochash(doc)
				self._freeshard(shard_id)

			if doc_id not in self.db["documents"]:
				raise Exception("doc_id", doc_id, "not found in", self.db["documents"])
			if "data" not in self.db["documents"][doc_id]:
				raise Exception("data not found in", self.db["documents"][doc_id])

			doc = self.db["documents"][doc_id]["data"]
			self.db["documents"][doc_id]["accessed"] = t.timestamp()

			self._indexdoc(doc)
		else:
			if self._haspeers():
				mirrorid = self._choosepeer()
				peerdoc = self.readdoc(mirrorid)
				rdoc = self._getremote(peerdoc["dbserver"] + "/Doc/" + doc_id)
				self.debugmsg(7, "rdoc:", rdoc)
				self._saveremotedoc(rdoc)

			if doc_id in self.db["documents"]:
				t = datetime.datetime.now()
				doc = self.db["documents"][doc_id]["data"]
				self.db["documents"][doc_id]["accessed"] = t.timestamp()
			else:
				raise Exception("doc", doc_id, "not found")

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
		# self.debugmsg(9, "index:", self.db["index"])
		# self.debugmsg(9, "index local:", self.db["index"]["local"])
		if "index" not in self.db:
			self.db["index"] = {}
		if "local" not in self.db["index"]:
			self.db["index"]["local"] = {}
		# self.debugmsg(9, "index local keys:", self.db["index"]["local"].keys())
		# if doc_id in self.db["index"]["local"].keys():
		# 	self.debugmsg(6, "doc_id:", doc_id, ":", self.db["index"]["local"][doc_id])
		# 	return self.db["index"]["local"][doc_id]

		if doc_id in self.db["index"]["local"].keys():
			del self.db["index"]["local"][doc_id]

		shard_id = self._getshardid(doc_id)
		# self.debugmsg(9, "shard_id:", shard_id, "doc_id:", doc_id)
		if "shards" not in self.db:
			self.db["shards"] = {}
		if shard_id not in self.db["shards"]:
			self.db["shards"][shard_id] = self._loadshard(shard_id)
			# self.debugmsg(9, "shard_id:", shard_id, ":", self.db["shards"][shard_id])
			if len(self.db["shards"][shard_id])>0:
				for id in list(self.db["shards"][shard_id].keys()):
					# self.debugmsg(9, "id:", id, ":", self.db["shards"][shard_id][id])
					self.db["index"]["local"][id] = self.db["shards"][shard_id][id]["rev"]
				self._freeshard(shard_id)
			else:
				self._freeshard(shard_id)
				return None

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
