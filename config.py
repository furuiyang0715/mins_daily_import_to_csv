import os

env = os.environ.get


MONGOURL = env("MONGOURL", "mongodb://127.0.0.1:27017")