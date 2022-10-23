import random
from faker import Faker
import sys

sys.path.append('../MyNoSQL')
import MyNoSQL
import time

def getuserid(doc):
	uidlst = list(db.indexread("userid").values())
	# print("uidlst:", uidlst)
	name = doc["name"].split()
	initial = name[0][0:1]
	inittwo = name[0][0:2]
	tsname = name[1][0:5]
	addr = doc["address"].split()
	number = addr[0]
	uid = "{}{}".format(initial, tsname)
	if uid not in uidlst:
		return uid
	uid = "{}{}{}".format(initial, tsname, number)
	if uid not in uidlst:
		return uid
	uid = "{}{}".format(initial, name[1])
	if uid not in uidlst:
		return uid
	uid = "{}{}{}".format(initial, name[1], number)
	if uid not in uidlst:
		return uid
	uid = "{}{}".format(inittwo, tsname)
	if uid not in uidlst:
		return uid
	uid = "{}{}{}".format(inittwo, tsname, number)
	if uid not in uidlst:
		return uid
	uid = "{}{}".format(inittwo, name[1])
	if uid not in uidlst:
		return uid
	uid = "{}{}{}".format(inittwo, name[1], number)
	if uid not in uidlst:
		return uid
	uid = "{}{}".format(name[0], name[1])
	if uid not in uidlst:
		return uid
	uid = "{}{}{}".format(name[0], name[1], number)
	if uid not in uidlst:
		return uid

def newuser():
	fake = Faker()
	myobj = {}
	myobj["name"] = fake.name()
	myobj["address"] = fake.address()
	myobj["userid"] = getuserid(myobj)
	# print("myobj: ", myobj)
	return myobj

def updateuser(doc):
	fake = Faker()
	if "name" not in doc:
		doc["name"] = fake.name()
	if "address" not in doc:
		doc["address"] = fake.address()
	if "userid" not in doc:
		doc["userid"] = getuserid(doc)
	# print("doc: ", doc)
	return doc



db = MyNoSQL.MyNoSQL()
db.opendb("mydb5")
# print("db.db: ", db.db)
print("")

# fake = Faker()
# myobj = {}
# myobj["name"] = fake.name()
# print("myobj: ", myobj)
#
# myobj = db.savedoc(myobj)
# print("myobj: ", myobj)
#
# myobj["address"] = fake.address()
# myobj = db.savedoc(myobj)
# print("myobj: ", myobj)
#
# rev = db.indexread("rev")
# # print("rev: ", rev)
#
#
# # id = list(rev.keys())[0]
# id = random.choice(list(rev.keys()))
# print("rev 0: ", id)
# myobj2 = db.readdoc(id)
# print("myobj2: ", myobj2)
# myobj2["address"] = fake.address()
# myobj2 = db.savedoc(myobj2)
# print("myobj2: ", myobj2)
#
#
# # db.indexadd("name")
# # db.indexadd("address")
# # db.indexupdate()
# # db.indexdrop("name")
# # db.indexdrop("address")
#
# addr = db.indexread("name")
# print("addr: ", addr)
# # addr = db.indexread("address")
# # print("addr: ", addr)
# id = random.choice(list(addr.keys()))
# myobj3 = db.readdoc(id)
# print("myobj3: ", myobj3)
# myobj3["address"] = fake.address()
# myobj3 = db.savedoc(myobj3)
# print("myobj3: ", myobj3)
#


print("")
dbself = db.getselfdoc()
print("dbself:", dbself)

print("")
# if dbself["dbserver"] != "http://DavesMBPSG:8800":
# 	db.addpeer("http://DavesMBPSG:8800")
# if dbself["dbserver"] != "http://DavesMBPSG:8801":
# 	db.addpeer("http://DavesMBPSG:8801")

db.addpeer("http://MyNoSQL-M1:8800")


time.sleep(5)

print("")
print("")
print("")
print("")
print("")

for i in range(3):
	myobj = newuser()
	myobj = db.savedoc(myobj)
	print("myobj: ", myobj)
	time.sleep(5)
#
#
rev = db.indexread("rev")
for i in range(3):
	print("")
	id = random.choice(list(rev.keys()))
	print("id: ", id)
	myobj2 = db.readdoc(id)
	print("myobj2: ", myobj2)
	# lb4 = len(myobj2)
	# myobj2a = updateuser(myobj2)
	# print("myobj2a: ", myobj2a)
	# print("len(myobj2a): ", len(myobj2a), "	lb4:", lb4)
	# if len(myobj2a) != lb4:
	# 	myobj2 = db.savedoc(myobj2a)
	# 	print("saved myobj2: ", myobj2)
	time.sleep(5)


time.sleep(60)



print("")
print("db.db: ", db.db)
print("")
db.closedb()
print("")
print("db.db: ", db.db)
print("")
