import random
from faker import Faker
import sys

sys.path.append('../MyNoSQL')
import MyNoSQL

import time



db = MyNoSQL.MyNoSQL()
db.opendb("mydb6")
# print("db.db: ", db.db)
print("")
db.getdbmode()
db.setdbmode("Mirror")



for i in range(5):
	time.sleep(60)




print("")
print("db.db: ", db.db)
print("")
db.closedb()
print("")
print("db.db: ", db.db)
print("")
