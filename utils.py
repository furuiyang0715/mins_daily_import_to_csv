import datetime
import functools
import json
import logging
import os
import shutil
import subprocess
import sys
import time

import pymongo
import pandas as pd
import pymysql
import schedule
from raven import Client
from sqlalchemy import create_engine

from config import MONGOURL, MYSQLHOST, MYSQLUSER, MYSQLPASSWORD, MYSQLPORT, MYSQLDB, MYSQLTABLE, SENTRY_DSN

logger = logging.getLogger("main_log")
db = pymongo.MongoClient(MONGOURL)
coll = db.stock.mins
sentry = Client(SENTRY_DSN)


def all_codes_now():
    codes = coll.find().distinct("code")
    return codes  # 12332  12359 果然每天的 codes 数量不一样


def write_codes_to_file(codes):
    """如果codes不是经常有新增的 查询一次写入文件
    下次需要的时候直接从文件中读取要比数据库 distinct 查询要快"""
    with open("codes.json", "w") as f:
        json.dump(codes, f)


def wirte_code_date_to_file(dt1, dt2, date_int_str):
    """
    将指定 code 制定时间内的增量 写入文件
    :param dt1:
    :param dt2:
    :param date_int_str:
    :return:
    """
    f = open("codes.json", "r")
    codes = json.load(f)
    f.close()

    file_path = os.path.join(os.getcwd(), "exportdir/" + date_int_str)
    os.makedirs(file_path, exist_ok=True)

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
    count = 0
    try:
        for i in range(1, len(file_list)):
            # print(os.path.join(Folder_Path, file_list[i]))
            df = pd.read_csv(os.path.join(folder_path, file_list[i]))
            # print(df)
            # print(df.shape[0])
            count += df.shape[0]

            # print()
            # print()

            df.to_csv(save_file, encoding="utf-8", index=False, header=False, mode='a+')
    except Exception:
        pass

    return count


def csv_to_mysql(load_sql, host, user, password):
    """
    This function load a csv file to MySQL table according to
    the load_sql statement.
    :param load_sql:
    :param host:
    :param user:
    :param password:
    :return:
    """
    try:
        con = pymysql.connect(host=host,
                              user=user,
                              password=password,
                              autocommit=True,
                              local_infile=1)
        print('Connected to DB: {}'.format(host))
        # Create cursor and execute Load SQL
        cursor = con.cursor()
        cursor.execute(load_sql)
        print('Succuessfully loaded the table from csv.')
        con.close()

    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)


def gen_times():
    """
    确定拉取的边界时间
    一般情况
    :return:
    """
    dt1 = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1),
                                    datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")
    dt2 = datetime.datetime.combine(datetime.date.today(),
                                    datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")
    date_int_str = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1),
                                             datetime.time.min).strftime("%Y%m%d")

    return dt1, dt2, date_int_str


def gen_temp_times(start, end):
    """
    需要补充数据的特殊情况
    end 是先对当前已经过去的时间
    :param start:
    :param end:
    :return:
    """
    while start.date() <= end.date():
        dt1 = datetime.datetime.combine(start, datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")
        dt2 = datetime.datetime.combine(start + datetime.timedelta(days=1),
                                        datetime.time.min).strftime("%Y-%m-%dT%H:%M:%SZ")
        date_int_str = datetime.datetime.combine(start, datetime.time.min).strftime("%Y%m%d")
        yield dt1, dt2, date_int_str
        start += datetime.timedelta(days=1)


# def gen_mongo_count(dt1, dt2):
#     """
#     计算在dt1 和 dt2之间的增量数量 理论上是一天的增量
#     :param dt1:
#     :param dt2:
#     :return:
#     """
#     # {
#     #     "_id": ObjectId("59ce1e1d6e6dc7768c7140dc"),
#     #     "code": "SH900955",
#     #     "time": ISODate("1999-07-26T09:59:00Z"),
#     #     "open": 0.462,
#     #     "close": 0.462,
#     #     "low": 0.462,
#     #     "high": 0.462,
#     #     "volume": 0,
#     #     "amount": 0
#     # }
#     # 现将 dt1 和 dt2 进行转换
#     ret = coll.find({"time": {"$gte": dt1, "$lte": dt2}}).count_documents
#     return ret


def gene(dt1, dt2, date_int_str):
    """整个生成逻辑"""
    logger.info(f"dt1:{dt1}")
    logger.info(f"dt2:{dt2}")

    mysqlhost = MYSQLHOST
    user = MYSQLUSER
    password = MYSQLPASSWORD
    mysqlport = MYSQLPORT
    mysqldb = MYSQLDB
    mysqltable = MYSQLTABLE

    export_path = os.path.join(os.getcwd(), "exportdir")
    folder_path = os.path.join(export_path, date_int_str)
    save_file_path = os.path.join(os.getcwd(), "savedir")
    save_file_name = date_int_str + ".csv"

    # 生成截止到当前的全种类列表
    codes = all_codes_now()

    # 将其以重写的方式存入 codes.json 文件
    write_codes_to_file(codes)

    # 将 codes 读出到内存 同时将每一个code的增量写入文件
    wirte_code_date_to_file(dt1, dt2, date_int_str)

    # 将 csv 文件进行合并 并且计算被导入的增量数量
    count = merge_csv(folder_path, save_file_path, save_file_name)
    logger.info(f"由csv文件计算出的当天需要进行增量的数据量为 {count}")
    sentry.captureMessage(f"需要进行增量的数据量为 {count}")

    # 检查与 mongo 中的增量结果是否一致
    # 这个查询也比较耗时 先不检查了
    # mongo_count = gen_mongo_count(dt1, dt2)

    if count:
        # 将合并后的 csv 导入 mysql
        save_file = os.path.join(save_file_path, save_file_name)
        load_sql = f"""LOAD DATA LOCAL INFILE '{save_file}' \
        REPLACE INTO TABLE {mysqldb}.{mysqltable} \
        FIELDS TERMINATED BY ',' \
        ENCLOSED BY '"' \
        IGNORE 1 LINES;"""

        csv_to_mysql(load_sql, mysqlhost, user, password)

        # 检查 csv 中的数目和数据库中查询出的数量是否一致
        query_sql = f"""select count(1) from {mysqldb}.{mysqltable} where time >= {date_int_str}"""
        mysql_string = f"mysql+pymysql://{user}:{password}@{mysqlhost}:\
        {mysqlport}/{mysqldb}?charset=gbk"
        DATACENTER = create_engine(mysql_string)
        sql_count = DATACENTER.execute(query_sql).first()[0]

        if sql_count != count:
            raise RuntimeError("数据量不一致，请检查！")

    # 合并后删除原始的 csv 文件
    shutil.rmtree(folder_path, ignore_errors=True)
    logger.info(f"任务完成 删除当日过程 csv 文件 ")


def catch_exceptions(cancel_on_failure=False):
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                import traceback
                logger.warning(traceback.format_exc())
                sentry.captureException(exc_info=True)
                if cancel_on_failure:
                    # print(schedule.CancelJob)
                    # schedule.cancel_job()
                    return schedule.CancelJob
        return wrapper
    return catch_exceptions_decorator


@catch_exceptions
def main():
    import_date_str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"现在是 {datetime.datetime.today()}, 开始增量 stock.mins 在 "
                f"{import_date_str} 全天的增量数据到 mysql 数据库")
    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始增量 stock.mins 在 "
                          f"{import_date_str} 全天的增量数据到 mysql 数据库")
    dt1, dt2, date_int_str = gen_times()
    gene(dt1, dt2, date_int_str)


if __name__ == "__main__":
    # test gen times
    # res = gen_times()
    # print(res)

    # test gen temp times
    # start = datetime.datetime(2019, 7, 8, 12, 34, 56)
    # end = datetime.datetime(2019, 7, 18, 12, 34, 56)
    # generator = gen_temp_times(start, end)
    # for data in generator:
    #     print(data)
    # """
    # ('2019-07-08T00:00:00Z', '2019-07-09T00:00:00Z', '20190708')
    # ('2019-07-09T00:00:00Z', '2019-07-10T00:00:00Z', '20190709')
    # ('2019-07-10T00:00:00Z', '2019-07-11T00:00:00Z', '20190710')
    # ('2019-07-11T00:00:00Z', '2019-07-12T00:00:00Z', '20190711')
    # ('2019-07-12T00:00:00Z', '2019-07-13T00:00:00Z', '20190712')
    # ('2019-07-13T00:00:00Z', '2019-07-14T00:00:00Z', '20190713')
    # ('2019-07-14T00:00:00Z', '2019-07-15T00:00:00Z', '20190714')
    # ('2019-07-15T00:00:00Z', '2019-07-16T00:00:00Z', '20190715')
    # ('2019-07-16T00:00:00Z', '2019-07-17T00:00:00Z', '20190716')
    # ('2019-07-17T00:00:00Z', '2019-07-18T00:00:00Z', '20190717')
    # ('2019-07-18T00:00:00Z', '2019-07-19T00:00:00Z', '20190718')
    # """

    # test gen all codes from mongo today
    # t1 = time.time()
    # all_codes_now()
    # print(time.time() - t1)  # 61s

    # test wirte code to a file
    # t1 = time.time()
    # now_codes = all_codes_now()
    # write_codes_to_file(now_codes)
    # print(time.time() - t1)  # 55s

    # 写入分散的 csv 文件
    dt1, dt2, date_int_str = gen_times()
    # t1 = time.time()
    # wirte_code_date_to_file(dt1, dt2, date_int_str)
    # t2 = time.time()
    # print((t2 - t1)/60, "min")  # 79min

    # test gen momngo count
    # mongo_count = gen_mongo_count(dt1, dt2)
    # print(mongo_count)

    # test merge csv
    # dt1, dt2, date_int_str = gen_times()
    export_path = os.path.join(os.getcwd(), "exportdir")
    folder_path = os.path.join(export_path, date_int_str)
    # save_file_path = os.path.join(os.getcwd(), "savedir")
    # save_file_name = date_int_str + ".csv"
    #
    # t1 = time.time()
    # count = merge_csv(folder_path, save_file_path, save_file_name)
    # t2 = time.time()
    # print(t2 - t1)  # 时间在 1 min 左右
    # print(count)  # 1309440

    # test csv to mysql

    # 将合并后的 csv 导入 mysql
    # t1 = time.time()
    # mysqlhost = MYSQLHOST
    # user = MYSQLUSER
    # password = MYSQLPASSWORD
    # mysqlport = MYSQLPORT
    # mysqldb = MYSQLDB
    # mysqltable = MYSQLTABLE
    # save_file = os.path.join(save_file_path, save_file_name)
    # load_sql = f"""LOAD DATA LOCAL INFILE '{save_file}' \
    #         REPLACE INTO TABLE {mysqldb}.{mysqltable} \
    #         FIELDS TERMINATED BY ',' \
    #         ENCLOSED BY '"' \
    #         IGNORE 1 LINES;"""
    #
    # csv_to_mysql(load_sql, mysqlhost, user, password)
    # print(time.time() - t1)  # 97s
    #
    # # 检查 csv 中的数目和数据库中查询出的数量是否一致
    # query_sql = f"""select count(1) from {mysqldb}.{mysqltable} where time >= '{dt1[:10]}' and time <= '{dt2[:10]}';"""
    # mysql_string = f"mysql+pymysql://{user}:{password}@{mysqlhost}:\
    #         {mysqlport}/{mysqldb}?charset=gbk"
    # print(query_sql)
    # DATACENTER = create_engine(mysql_string)
    # sql_count = DATACENTER.execute(query_sql).first()[0]
    # print(sql_count)  # 1309440

    # 测试删除合并后的文件夹
    # shutil.rmtree(folder_path, ignore_errors=True)

    pass
