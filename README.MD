### 

实现将 stock.mins 中每日的增量数据导出到 mysql 中 方便另外一个进程对数据进行清洗

### 操作步骤
#### 建表

账号信息: 
```angular2
ssh xuky@192.168.233.48
密码: xuky
```

登录mysql: 
```angular2
mysql -h 127.0.0.1 -P 4000 -uroot 无密码
```


建库建表，已经建立的忽略：
```angular2
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
```

#### 根据实际情况完善环境变量 config.env 
```angular2
MONGOURL=mongodb://127.0.0.1:27017
MYSQLHOST=127.0.0.1
MYSQLUSER =root
MYSQLPASSWORD=ruiyang
MYSQLPORT=3306
MYSQLDB=test01
MYSQLTABLE=inc_mins
SENTRY_DSN=https://330e494ccd22497db605a102491c0423@sentry.io/1501024
```

#### 构造镜像并推送到仓库
```angular2
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/incmins:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/incmins:v0.0.1
```

#### 启动服务 
```angular2
docker run -itd --name incrunner01 \
--env-file config.env \
registry.cn-shenzhen.aliyuncs.com/jzdev/incmins:v0.0.1
```
