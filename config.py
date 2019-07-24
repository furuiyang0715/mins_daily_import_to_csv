import os

env = os.environ.get


MONGOURL = env("MONGOURL", "mongodb://127.0.0.1:27017")

MYSQLHOST = env("MYSQLHOST", "localhost")
MYSQLUSER = env("MYSQLUSER", "root")
MYSQLPASSWORD = env("MYSQLPASSWORD", 'ruiyang')
MYSQLPORT = env("MYSQLPORT", 3306)
MYSQLDB = env("MYSQLDB", "test01")
MYSQLTABLE = env("MYSQLTABLE", 'inc_mins')