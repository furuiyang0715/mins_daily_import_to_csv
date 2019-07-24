# 将 csv 文件导入 mysql 的流程

"""
load data infile 'csv文件路径\\test.csv'    # 导入文件
replace into table 表名    # 导入到的数据表名 实现控制对现有唯一记录的重复处理机制： replace 和 ignore
fields terminated by ',' 描述字段的分隔符
optionally enclosed by '"' 字段的括起字符 如果字段中有引号 就当做是字段的一部分
lines terminated by '\n'   对每行进行分割
ignore 1 lines(Id,@dummy,DayOfWeek,PdDistrict,Address,X,Y);  # 忽略第一行 因为第一行往往是字段名 说明导入之前要提前建表

如果说 csv 文件里面有个字段不想插进去  就把对应的字段名变成 @dummy

"""
# 数据字段
"""
{
	"_id" : ObjectId("59ce1e1d6e6dc7768c7140dc"),
	"code" : "SH900955",
	"time" : ISODate("1999-07-26T09:59:00Z"),
	"open" : 0.462,
	"close" : 0.462,
	"low" : 0.462,
	"high" : 0.462,
	"volume" : 0,
	"amount" : 0
}
"""

# 建表
"""
CREATE DATABASE stock;
CREATE TABLE IF NOT EXISTS `inc_mins`(
   `_id` VARCHAR(100) NOT NULL,
   `code` VARCHAR(100) NOT NULL,
   `time` VARCHAR(100) NOT NULL,
   `open` FLOAT DEFAULT NULL,
   `low` FLOAT DEFAULT NULL,
   `high` FLOAT DEFAULT NULL,
   `close` FLOAT DEFAULT NULL,
   `volume` text,
   `amount` text,
   UNIQUE KEY `_id` ( `_id` )
)ENGINE=MyISAM DEFAULT CHARSET=utf8;
"""

# 导入语句
# 20190720_10001718.csv

"""
load data infile '/Users/furuiyang/codes/mins_daily_import_to_csv/exportdir/20190720_10001718.csv' \
replace into table test01.inc_mins  \
fields terminated by ',' \
optionally enclosed by '"' \
lines terminated by '\n' \
ignore 1 lines(_id,code,time,open,low,high,close,volume,amount);
"""


# 尝试使用 python 程序生成
# 第一步 拼接 csv 文件
import pandas as pd
import os
import sys

Folder_Path = r'/Users/furuiyang/codes/mins_daily_import_to_csv/exportdir/20190720'  # 要拼接的文件夹及其完整路径，注意不要包含中文
SaveFile_Path = r'/Users/furuiyang/codes/mins_daily_import_to_csv/savedir/20190720'  # 拼接后要保存的文件路径
SaveFile_Name = r'20190720.csv'  # 合并后要保存的文件名

# 修改当前工作目录
os.chdir(Folder_Path)
# 将该文件夹下的所有文件名存入一个列表
file_list = os.listdir()
# print(file_list)
# sys.exit(0)

# 读取第一个CSV文件并包含表头
df = pd.read_csv(os.path.join(Folder_Path, file_list[0]))
# print(df)
# sys.exit(0)

# 创建要保存的文件夹
os.makedirs(SaveFile_Path, exist_ok=True)

# 将读取的第一个CSV文件写入合并后的文件保存
save_file = os.path.join(SaveFile_Path, SaveFile_Name)
df.to_csv(save_file)
# sys.exit(0)

# 循环遍历列表中各个CSV文件名，并追加到合并后的文件
count = 0
try:
    for i in range(1, len(file_list)):
        print(os.path.join(Folder_Path, file_list[i]))
        df = pd.read_csv(os.path.join(Folder_Path, file_list[i]))
        print(df)
        print(df.shape[0])
        count += df.shape[0]

        print()
        print()
        try:
            df.to_csv(save_file, encoding="utf-8", index=False, header=False, mode='a+')
        except Exception as e:
            print(e)
            sys.exit(0)
except Exception:
    print("wrong.")
    pass

print(count)   # 2599680



# import pymysql
#
# conn = pymysql.Connect(host='localhost', port=3306, user='root', password='ruiyang', db='test01', charset='utf8')
# cursor = conn.cursor()
#
# # 建表
# # sql1 = "create table coupon(id INT(4) NOT NULL auto_increment,\
# # coupon_num VARCHAR(255) not null,primary key(id))"
# # cursor.execute(sql1)
#
# # 插入执行
# # sql2 = "insert into inc_mins (_id,code,time,open,close,low,high,volume,amount) values {},{},{},{},{},{},{},{},{}"
# sql2 = "insert into inc_mins (_id,code,time,open,close,low,high,volume,amount) values {};"
# file = open('/Users/furuiyang/codes/mins_daily_import_to_csv/savedir/20190720/20190720_001.csv','r')
# file.readline()
#
# for i in file.readlines():
#     data = i.strip().split(',')
#     print(data)
#     print(sql2.format(tuple(data)))
#     cursor.execute(sql2.format(tuple(data)))


# 使用 load file 的形式去调用
import pymysql


def csv_to_mysql(load_sql, host, user, password):
    '''
    This function load a csv file to MySQL table according to
    the load_sql statement.
    '''
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


# Execution Example
load_sql = """LOAD DATA LOCAL INFILE '/Users/furuiyang/codes/mins_daily_import_to_csv/savedir/20190720/20190720_001.csv' \
REPLACE INTO TABLE test01.inc_mins \
FIELDS TERMINATED BY ',' \
ENCLOSED BY '"' \
IGNORE 1 LINES;"""

print(load_sql)


"""
load data infile '/Users/furuiyang/codes/mins_daily_import_to_csv/exportdir/20190720_10001718.csv' \
replace into table test01.inc_mins  \
fields terminated by ',' \
optionally enclosed by '"' \
lines terminated by '\n' \
ignore 1 lines(_id,code,time,open,low,high,close,volume,amount);
"""


host = 'localhost'
user = 'root'
password = 'ruiyang'
csv_to_mysql(load_sql, host, user, password)

