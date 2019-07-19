import json

import pymongo

from config import MONGOURL

db = pymongo.MongoClient(MONGOURL)

coll = db.stock.mins


def all_codes_now():
    codes = coll.find().distinct("code")
    return codes  # 12332


def write_codes_to_file(codes):
    """如果codes不是经常有新增的 查询一次写入文件 下次需要的时候直接从文件中读取要比数据库 distinct 查询要快"""
    with open("codes.py", "w") as f:
        json.dump(codes, f)


if __name__ == "__main__":
    # now_codes = all_codes_now()
    # write_codes_to_file(now_codes)

    # # codes 的读出
    # f = open("codes.py", "r")
    # codes = json.load(f)
    # print(codes)
    # print(len(codes))
    # print(type(codes))

    pass



