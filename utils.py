import datetime
import json
import os
import subprocess
import time

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


def wirte_code_date_to_file():
    # 确定同步的边界时间
    # 开始时间是昨天的零点 结束时间是今天的零点
    dt1 = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1), datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")
    dt2 = datetime.datetime.combine(datetime.date.today(), datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 同步的是昨日的增量数据 文件夹以昨天命名
    date_int_str = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1), datetime.time.min).strftime("%Y%m%d")

    file_path = os.path.join(os.getcwd(), "exportdir/" + date_int_str)
    os.makedirs(file_path, exist_ok=True)

    f = open("codes.py", "r")
    codes = json.load(f)
    f.close()

    for code in codes:
        q = '{{code:"{0}",time: {{$gte:ISODate("{1}"), $lte:ISODate("{2}")}}}}'.format(code, dt1, dt2)
        file_name = os.path.join(file_path, code)
        command = "mongoexport -d stock -c mins -q '{}' --fieldFile mins_fields.txt --type=csv --out {}.csv".format(q, file_name)
        # print(command)
        log_file = open("export_log.log", "a+")
        subprocess.call(command, shell=True, stderr=log_file)


if __name__ == "__main__":
    # now_codes = all_codes_now()
    # write_codes_to_file(now_codes)

    # # codes 的读出
    # f = open("codes.py", "r")
    # codes = json.load(f)
    # print(codes)
    # print(len(codes))
    # print(type(codes))
    t1 = time.time()
    wirte_code_date_to_file()
    t2 = time.time()
    print((t2 - t1)/60, "min")

    pass




