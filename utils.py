import datetime
import json
import os
import subprocess
import sys
import time

import pymongo
import pandas as pd

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
    dt1 = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1),
                                    datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")
    dt2 = datetime.datetime.combine(datetime.date.today(),
                                    datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 同步的是昨日的增量数据 文件夹以昨天命名
    date_int_str = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1),
                                             datetime.time.min).strftime("%Y%m%d")

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


def merge_csv(folder_path, savefile_path, savefile_name):
    """
    将多个 csv 文件合并成一个
    :param folder_path:
    :param savefile_path:
    :param savefile_name:
    :return:
    """
    # 修改当前工作目录
    os.chdir(folder_path)
    # 将该文件夹下的所有文件名存入一个列表
    file_list = os.listdir()

    # 读取第一个CSV文件并包含表头
    df = pd.read_csv(os.path.join(folder_path, file_list[0]))

    # 创建要保存的文件夹
    os.makedirs(savefile_path, exist_ok=True)

    # 将读取的第一个CSV文件写入合并后的文件保存
    save_file = os.path.join(savefile_path, savefile_name)
    df.to_csv(save_file)

    # 循环遍历列表中各个CSV文件名，并追加到合并后的文件
    # count = 0
    try:
        for i in range(1, len(file_list)):
            # print(os.path.join(Folder_Path, file_list[i]))
            df = pd.read_csv(os.path.join(folder_path, file_list[i]))
            # print(df)
            # print(df.shape[0])
            # count += df.shape[0]

            # print()
            # print()

            df.to_csv(save_file, encoding="utf-8", index=False, header=False, mode='a+')
    except Exception:
        pass


if __name__ == "__main__":
    # now_codes = all_codes_now()
    # write_codes_to_file(now_codes)

    # # codes 的读出
    # f = open("codes.py", "r")
    # codes = json.load(f)
    # print(codes)
    # print(len(codes))
    # print(type(codes))
    # t1 = time.time()
    # wirte_code_date_to_file()
    # t2 = time.time()
    # print((t2 - t1)/60, "min")

    # test merge csv
    # t1 = time.time()
    # folder_path = "/Users/furuiyang/codes/mins_daily_import_to_csv/exportdir/20190720"
    # save_file_path = "/Users/furuiyang/codes/mins_daily_import_to_csv/savedir/20190720"
    # save_file_name = "20190720.csv"
    # merge_csv(folder_path, save_file_path, save_file_name)
    # t2 = time.time()
    # print(t2 - t1)

    pass





