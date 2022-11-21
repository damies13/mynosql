import random
from faker import Faker
import sys

sys.path.append('../MyNoSQL')
import MyNoSQL
import time



db = MyNoSQL.MyNoSQL()
db.opendb("mydb7")
# print("db.db: ", db.db)
db.debuglvl = 5

print("")
dbself = db.getselfdoc()
print("dbself:", dbself)

print("")

# if dbself["dbserver"] != "http://DavesMBPSG:8800":
# 	db.addpeer("http://DavesMBPSG:8800")
# if dbself["dbserver"] != "http://DavesMBPSG:8801":
# 	db.addpeer("http://DavesMBPSG:8801")

db.addpeer("http://MyNoSQL-M2:8800")


time.sleep(5)

print("")

time.sleep(60)
print("Attempting to read random records")

rev = db.indexread("rev")
print("rev:", rev)
for i in range(3):
	print("")
	print("keys:",list(rev.keys()))
	id = random.choice(list(rev.keys()))
	print("Attempting to read a random record with id: ", id)
	print("id: ", id)
	myobj7 = db.readdoc(id)
	print("myobj7: ", myobj7)
	time.sleep(10)
	time.sleep(50)




time.sleep(60)



print("")
print("db.db: ", db.db)
print("")
db.closedb()
print("")
# print("db.db: ", db.db)
# print("")
