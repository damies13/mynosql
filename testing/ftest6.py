import random
# from faker import Faker
import sys

sys.path.append('../MyNoSQL')
import MyNoSQL

import time

keeprunning = True

db = MyNoSQL.MyNoSQL()
db.opendb("mydb6")

# db.debuglvl = 9

# print("db.db: ", db.db)
print("")
db.getdbmode()
db.setdbmode("Mirror")

# db.addpeer("http://MyNoSQL-M2:8800")
# db.addpeer("http://MyNoSQL-M1:8800")



# for i in range(15):
# 	time.sleep(60)

while keeprunning:
	try:
		time.sleep(60)
	except KeyboardInterrupt:
		keeprunning = False



print("")
print("db.db: ", db.db)
print("")
db.closedb()
print("")
# print("db.db: ", db.db)
# print("")
