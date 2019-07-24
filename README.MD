### 

实现将 stock.mins 中每日的增量数据导出到 mysql 中 方便另外一个进程对数据进行清洗

### 操作步骤
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
